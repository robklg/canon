//! **The read-back round trip**, checked: every row `canon facts` prints, pasted as printed onto
//! the same selection in the same zone, selects exactly the sources it counted; a marked row's
//! line selects exactly them (`+`) or more (`!`); a key of mixed type's rows are refused when
//! pasted, never claimed to read back.

use std::collections::{BTreeMap, BTreeSet};

use crate::core::domain::time::zone::UserZone;
use crate::expr::RowFilter;
use crate::expr::{fetch_row_values, ParsedFactKey};
use crate::facts::ops::report::{
    compute_all_keys, compute_root_distribution, Group, Line, Mark, Part, Position, Row,
};

use super::fixtures::{cells_of, counted, Library};
use crate::facts::ops::report::Distribution;

/// The text after a line's `: `, or the line's whole sentence.
fn after_colon(line: &Line) -> &str {
    line.filter.as_deref().unwrap_or(&line.label)
}

/// **One distribution, pasted back**: every row's filter and every line, run as `canon ls`
/// would run it. An exact row selects exactly the sources it counted, and its filter is
/// `<header>=<row>` where it is unmarked; a `+` row's line, its value written in, selects exactly
/// them; a `!` row's glob selects them and perhaps more; `(no value)`'s line selects exactly the
/// sources with none; a key of mixed type's row, pasted, is refused. The rows add up to the
/// selection.
fn check_distribution(library: &mut Library, key_text: &str) {
    let report = library.distribution(key_text, 0);
    let key = library.read(Some(key_text), &[], false).main.unwrap();
    let counted = counted(library, &key);
    let all = library.all();
    let rows: Vec<&Row> = report.rows.iter().chain(&report.no_value).collect();
    let total: i64 = rows.iter().map(|row| row.count).sum();
    assert_eq!(total, all.len() as i64, "--key {key_text}: rows add up");
    // The same rows in the same order on every run, so `--limit` always keeps the same ones; a
    // row is its cell and its type, since values of two types can print alike.
    let order = |report: &Distribution| -> Vec<(String, Option<&'static str>, i64)> {
        report
            .rows
            .iter()
            .chain(&report.no_value)
            .map(|row| (row.cell.clone(), row.type_word, row.count))
            .collect()
    };
    for _ in 0..3 {
        let again = library.distribution(key_text, 0);
        assert_eq!(order(&again), order(&report), "--key {key_text}: order");
    }

    for row in &rows {
        let ids = &counted[&(row.cell.clone(), row.type_word)];
        assert_eq!(
            ids.len() as i64,
            row.count,
            "--key {key_text}: {}",
            row.cell
        );
        match &row.filter {
            None => {
                let pasted = format!("{}={}", report.header, row.cell);
                assert!(library.refuses(&pasted), "{pasted} is refused");
            }
            Some(RowFilter::Exact(text)) => {
                assert_eq!(&library.selected(&[text]), ids, "--key {key_text}: {text}");
                if row.mark.is_none() && row.cell != "(no value)" {
                    assert_eq!(text, &format!("{}={}", report.header, row.cell));
                }
            }
            Some(RowFilter::Wider(text)) => {
                assert!(
                    library.selected(&[text]).is_superset(ids),
                    "--key {key_text}: {text}"
                );
            }
        }
        match row.mark {
            Some(Mark::Coarse) => {
                // The mark is owed: the row's own value, pasted, selects more than it counted.
                let plain = library.selected(&[&format!("{}={}", report.header, row.cell)]);
                assert!(
                    plain.is_superset(ids) && plain != *ids,
                    "--key {key_text}: {} is marked + but pastes exactly",
                    row.cell
                );
                let exact = report.lines.iter().any(|line| {
                    line.label.ends_with("row marked +")
                        && library.selected(&[&after_colon(line).replace("<row>", &row.cell)])
                            == *ids
                });
                assert!(
                    exact,
                    "--key {key_text}: a line selects {} exactly",
                    row.cell
                );
            }
            Some(Mark::Unshowable) => {
                let label = format!("Row ! {} (may select more)", row.cell);
                let line = report
                    .lines
                    .iter()
                    .find(|line| line.label == label)
                    .unwrap_or_else(|| panic!("--key {key_text}: no line {label}"));
                assert!(library.selected(&[after_colon(line)]).is_superset(ids));
            }
            None => {}
        }
    }
    if let Some(no_value) = &report.no_value {
        let line = report
            .lines
            .iter()
            .find(|line| line.label == "(no value)")
            .expect("the (no value) line");
        assert_eq!(
            library.selected(&[after_colon(line)]),
            counted[&("(no value)".to_string(), None)]
        );
        assert_eq!(
            no_value.count,
            counted[&("(no value)".to_string(), None)].len() as i64
        );
    }
}

/// Each source's place in a grouped table: its group's part, and its row's parts.
fn grouped_counts(
    library: &mut Library,
    main: &ParsedFactKey,
    positions: &[Position],
) -> BTreeMap<(Part, Vec<Part>), BTreeSet<i64>> {
    let ids = library.all();
    let fetched = crate::core::repo::source::batch_fetch_by_ids(&library.conn, &ids).unwrap();
    let sources: Vec<_> = ids.iter().map(|id| &fetched[id]).collect();
    let mut keys = vec![main];
    keys.extend(positions.iter().filter_map(|position| match position {
        Position::Key(key) => Some(key),
        Position::Root => None,
    }));
    let values = fetch_row_values(&mut library.conn, &sources, &keys, &library.zone).unwrap();
    let mut out: BTreeMap<(Part, Vec<Part>), BTreeSet<i64>> = BTreeMap::new();
    for source in &sources {
        let mut read = values[&source.id].iter();
        let cell = |key: &ParsedFactKey, value: &Option<crate::expr::RowValue>| match value {
            Some(value) => Part {
                cell: key.row(value).cell,
                type_word: key.mixed_types().map(|_| value.type_word()),
            },
            None => Part {
                cell: "(no value)".to_string(),
                type_word: None,
            },
        };
        let group = cell(main, read.next().unwrap());
        let parts = positions
            .iter()
            .map(|position| match position {
                Position::Root => Part {
                    cell: format!(
                        "id:{}  {}",
                        source.root_id,
                        crate::expr::pictured(&source.root_path)
                    ),
                    type_word: None,
                },
                Position::Key(key) => cell(key, read.next().unwrap()),
            })
            .collect();
        out.entry((group, parts)).or_default().insert(source.id);
    }
    out
}

/// **A line filled as a person fills it**, from the printed row: `<value>` the group's cell,
/// `<row>` the one position's cell, `<1st>`, `<2nd>` … the k-th cell on the row, the root counted,
/// and `<N>` the root's id read off its cell. `None` where a placeholder would take `(no value)`,
/// which the row's own `(no value)` line writes out instead.
fn filled(line: &str, group: &str, cells: &[String], by_root: bool) -> Option<String> {
    let mut text = line.replace("<value>", group);
    for (at, cell) in cells.iter().enumerate() {
        if by_root && at == 0 {
            let id = cell
                .strip_prefix("id:")
                .unwrap()
                .split("  ")
                .next()
                .unwrap();
            text = text.replace("<N>", id);
            continue;
        }
        let placeholder = if cells.len() == 1 {
            "<row>".to_string()
        } else {
            ordinal(at + 1)
        };
        if text.contains(&placeholder) {
            if cell == "(no value)" {
                return None;
            }
            text = text.replace(&placeholder, cell);
        }
    }
    Some(text)
}

fn ordinal(n: usize) -> String {
    let suffix = match (n % 10, n % 100) {
        (1, 11) | (2, 12) | (3, 13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };
    format!("<{n}{suffix}>")
}

/// **One grouped table, pasted back**: each group's filter selects its rows' sources; each row's
/// filter exactly its own (more, for `!`); and each row is reached through the line a person
/// picks for it, filled from its printed cells — `Each indented row`, a `(no value)` row's own,
/// the `(no value)` group's, a `+` row's, a `!` row's — selecting exactly its sources, or more
/// for `!`.
fn check_grouped(library: &mut Library, key: &str, grouping: &[&str], by_root: bool) {
    let grouped = library.grouped(key, grouping, by_root, 0);
    let keys = library.read(Some(key), grouping, by_root);
    let main = keys.main.unwrap();
    let counts = grouped_counts(library, &main, &keys.grouping);
    let all = library.all();
    let groups: Vec<&Group> = grouped
        .groups
        .iter()
        .chain(&grouped.no_value_group)
        .collect();
    let total: i64 = groups.iter().map(|group| group.total).sum();
    assert_eq!(
        total,
        all.len() as i64,
        "{key} by {grouping:?}: groups add up"
    );

    for group in groups {
        let under_no_value = group.part.cell == "(no value)";
        let mut group_ids = BTreeSet::new();
        for row in &group.rows {
            let ids = &counts[&(group.part.clone(), row.parts.clone())];
            let cells = cells_of(&row.parts);
            assert_eq!(
                ids.len() as i64,
                row.count,
                "{} / {:?}",
                group.part.cell,
                cells
            );
            group_ids.extend(ids.iter().copied());
            match &row.filter {
                Some(RowFilter::Exact(text)) => {
                    assert_eq!(&library.selected(&[text]), ids, "{text}")
                }
                Some(RowFilter::Wider(text)) => {
                    assert!(library.selected(&[text]).is_superset(ids), "{text}")
                }
                None => continue,
            }
            let suffix = if under_no_value {
                " under (no value)"
            } else {
                ""
            };
            let candidates: Vec<&Line> = grouped
                .lines
                .iter()
                .filter(|line| match row.mark {
                    Some(Mark::Unshowable) => {
                        line.label
                            == format!(
                                "Row ! {} / {} (may select more)",
                                group.part.cell,
                                cells.join(" / ")
                            )
                    }
                    Some(Mark::Coarse) => {
                        line.label.starts_with("Each indented ")
                            && line.label.ends_with(&format!("row marked +{suffix}"))
                    }
                    None if cells.iter().any(|cell| cell == "(no value)") => {
                        line.label == format!("Each indented (no value) row{suffix}")
                    }
                    None => line.label == format!("Each indented row{suffix}"),
                })
                .collect();
            let reached = candidates.iter().any(|line| {
                let Some(text) = filled(after_colon(line), &group.part.cell, &cells, by_root)
                else {
                    return false;
                };
                let selected = library.selected(&[&text]);
                match row.mark {
                    Some(Mark::Unshowable) => selected.is_superset(ids),
                    Some(Mark::Coarse) | None => selected == *ids,
                }
            });
            assert!(
                reached,
                "{key} by {grouping:?}: no line reaches {} / {:?} among {:?}",
                group.part.cell,
                cells,
                candidates
                    .iter()
                    .map(|line| after_colon(line))
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(group_ids.len() as i64, group.total);
        match &group.filter {
            Some(RowFilter::Exact(text)) => {
                assert_eq!(library.selected(&[text]), group_ids, "{text}")
            }
            Some(RowFilter::Wider(text)) => {
                assert!(library.selected(&[text]).is_superset(&group_ids), "{text}")
            }
            None => {}
        }
        if group.mark == Some(Mark::Coarse) {
            let exact = grouped.lines.iter().any(|line| {
                line.label.ends_with("group marked +")
                    && library.selected(&[&after_colon(line).replace("<value>", &group.part.cell)])
                        == group_ids
            });
            assert!(exact, "the group {} is reached exactly", group.part.cell);
        }
        if group.mark == Some(Mark::Unshowable) {
            let label = format!("Row ! {} (may select more)", group.part.cell);
            let line = grouped
                .lines
                .iter()
                .find(|line| line.label == label)
                .unwrap_or_else(|| panic!("no line {label}"));
            assert!(library
                .selected(&[after_colon(line)])
                .is_superset(&group_ids));
        }
        if under_no_value {
            let line = grouped
                .lines
                .iter()
                .find(|line| line.label == "(no value)")
                .expect("the (no value) group's line");
            assert_eq!(library.selected(&[after_colon(line)]), group_ids);
        }
    }
}

/// **The per-root table, pasted back**: each root row's filter selects that root's sources, and
/// the root line, copied whole after `: ` with `<N>` filled, the same.
fn check_roots(library: &mut Library) {
    let ids = library.all();
    let fetched = crate::core::repo::source::batch_fetch_by_ids(&library.conn, &ids).unwrap();
    let sources: Vec<_> = fetched.values().cloned().collect();
    let table = compute_root_distribution(&sources);
    let line = &table.lines[0];
    for row in &table.rows {
        let id: i64 = row
            .cell
            .strip_prefix("id:")
            .unwrap()
            .split("  ")
            .next()
            .unwrap()
            .parse()
            .unwrap();
        let mine: BTreeSet<i64> = sources
            .iter()
            .filter(|source| source.root_id == id)
            .map(|source| source.id)
            .collect();
        assert_eq!(row.count, mine.len());
        assert_eq!(library.selected(&[row.filter.text()]), mine);
        let pasted = after_colon(line).replace("<N>", &id.to_string());
        assert_eq!(library.selected(&[&pasted]), mine);
    }
}

/// **The key list, pasted back**: each key printed bare or in Canon's quotes, typed into
/// `<key>?`, selects as many sources as the list counted; a key it marks cannot be pasted and is
/// not tried.
fn check_key_list(library: &mut Library) {
    let ids = library.all();
    let list = compute_all_keys(&mut library.conn, &ids, true).unwrap();
    for key in list.keys.iter().filter(|key| !key.mark) {
        let asked = library.selected(&[&format!("{}?", key.cell)]);
        assert_eq!(asked.len() as i64, key.count, "{}?", key.cell);
    }
}

/// The modification time 2024-01-01 00:00 UTC.
const NEW_YEAR: i64 = 1_704_067_200;

/// **The library every plant of the round trip lives in**, read in Amsterdam: text of every
/// shape a value can take, numbers, times at every precision and in every form, sizes for every
/// bucket, file names and paths with spaces and control characters, a second root, keys holding a
/// space, a content fact named `root`, a key alias, and values holding control characters.
fn planted() -> Library {
    let mut library = Library::in_zone(UserZone::for_test("Europe/Amsterdam"))
        .with_aliases("taken = \"source.mtime\"\n");
    let c1 = char::from_u32(0x85).unwrap();
    let long = "Ünïcødé — a value longer than any column a report ever printed, ".repeat(2);
    for value in [
        "Canon EOS 5D Mark II",
        "Canon EOS 5D Mark II",
        "say \"hi\"",
        " a ",
        "",
        "(no value)",
        "icon@2x.png",
        "a?b",
        "[draft]",
        long.as_str(),
        "2019",
        "2024-06-15",
        "18:00",
        "0500",
        "2019:05:04 10:12:00",
        "01234567",
        "1e400",
        "it's",
        "AND",
        "DMC-FZ38",
    ] {
        library.text("content.V", value);
    }
    for value in [
        "line1\nline2".to_string(),
        "x\n*?[\"y".to_string(),
        "crlf\r\nname".to_string(),
        format!("next{c1}line"),
    ] {
        library.text("content.Note", &value);
    }
    for n in [1024.0, 0.1, 18_446_744_073_709_551_616.0, -3.0, 9.0, 10.0] {
        library.number("content.N", n);
    }
    for n in [0.000125, 0.00005, 0.0, -2.0, 61.0, 400.0] {
        library.number("content.Small", n);
    }
    library.number("content.File Size", 7.0);
    library.number("content.File Size", 8.0);
    library.text("content.root", "x");
    library.text("content.Mixed", "a");
    library.number("content.Mixed", 5.0);
    // Values of three types that render alike: three rows, each its own type.
    library.text("content.Mixed", "2019");
    library.number("content.Mixed", 2019.0);
    library.time("content.Mixed", "2019");
    // A source whose own row holds a value and whose content holds one Canon cannot read: it
    // has none, for a report and for `<key>?` alike.
    let unreadable = library.time("content.T", "2020-02-02");
    library
        .conn
        .execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision,
                                observed_at, observed_basis_rev)
             SELECT 'object', object_id, 'content.T', 253402315200, 'clock', 'second', 0, NULL
             FROM sources WHERE id = ?1",
            [unreadable],
        )
        .unwrap();
    for written in [
        "2024",
        "2024-06",
        "2024-06-15",
        "2024-06-15T14:30",
        "2024-06-15T14:30:05",
        "2024-06-15T14:30:05.25",
        "2023-12-31T23:00:00Z",
        "2024-01-01T00:00:00+01:00",
        "2024-01-01T00:00:00",
        "2024-10-27T00:30:00Z",
        "2024-10-27T01:30:00Z",
        "2024-03-31T02:30",
        "2025",
    ] {
        library.time("content.T", written);
    }
    for written in [
        "2024-07-23T18:30",
        "2024-07-24T18:30",
        "2024-07-23T18:30:45",
    ] {
        library.time("content.Taken", written);
    }
    let root = library.root;
    let backup = library.add_root("/backup drive");
    for (where_, rel, size, mtime) in [
        (root, "sub dir/x.jpg", 0, NEW_YEAR),
        (root, "a/b/c.JPG", 5, NEW_YEAR - 3_600),
        (root, "Makefile", 20_000, 1_293_840_000),
        (root, "dir/a\nb.jpg", 999_999, 1_325_376_000),
        (root, "far.jpg", 1, 253_402_300_799),
        (backup, "y.png", 1_000_000_000, NEW_YEAR),
        (backup, "z", 1000, 1_718_582_400),
    ] {
        library.file_in(where_, rel, size, mtime);
    }
    library
}

/// **The read-back round trip's verifier**: every row of every table kind, every mark and every
/// line, pasted back onto the same selection in the same zone (Europe/Amsterdam, so both
/// clock-change hours are in it).
#[test]
fn every_printed_row_selects_what_it_counted() {
    let mut library = planted();
    for key in [
        "content.V",
        "content.Note",
        "content.N",
        "content.Small|bucket",
        "content.Small|bucket(60,300)",
        "content.File Size",
        "\"content.File Size\"",
        "File Size",
        "content.T",
        "content.T|datetime",
        "content.T|date",
        "content.T|yearmonth",
        "content.T|year",
        "content.T|weekday",
        "content.T|quarter",
        "content.Taken|time",
        "content.Mixed",
        "content.root",
        "source.mtime",
        "mtime",
        "\"mtime\"",
        "@taken",
        "source.size",
        "size",
        "ext",
        "source.ext",
        "filename",
        "stem",
        "source.rel_path",
        "source.rel_path[:-1]",
        "source.path",
        "hash",
        "hash_short",
        "root_id",
        "source.root",
    ] {
        check_distribution(&mut library, key);
    }
    for (key, grouping, by_root) in [
        ("source.ext", vec!["source.mtime"], false),
        ("source.ext", vec!["size,content.V"], true),
        (
            "source.ext",
            vec!["\"content.File Size\",content.Small|bucket(60,300)"],
            false,
        ),
        ("content.V", vec!["source.ext"], false),
        ("content.T", vec!["source.ext"], false),
        ("source.ext", vec!["content.T"], false),
        ("content.Note", vec!["ext"], true),
        ("source.ext", vec!["content.root"], true),
        ("source.size", vec!["source.root"], false),
        ("source.ext", vec!["content.Mixed"], false),
        ("content.Mixed", vec!["ext"], false),
    ] {
        check_grouped(&mut library, key, &grouping, by_root);
    }
    check_roots(&mut library);
    check_key_list(&mut library);

    // A key written each way names the same key: with and without Canon's quotes, short.
    let spellings = [
        "content.File Size",
        "\"content.File Size\"",
        "'content.File Size'",
    ];
    for same in [&spellings[..], &["mtime", "\"mtime\"", "'mtime'"]] {
        let reports: Vec<Vec<(String, i64)>> = same
            .iter()
            .map(|key| super::fixtures::cells(&library.distribution(key, 0)))
            .collect();
        assert!(
            reports.windows(2).all(|pair| pair[0] == pair[1]),
            "{reports:?}"
        );
    }
    let grouped: Vec<Vec<String>> = spellings
        .iter()
        .map(|key| {
            let grouped = library.grouped("source.ext", &[key], false, 0);
            grouped
                .groups
                .iter()
                .flat_map(|group| {
                    group
                        .rows
                        .iter()
                        .map(|row| cells_of(&row.parts).join(" / "))
                })
                .collect()
        })
        .collect();
    assert!(
        grouped.windows(2).all(|pair| pair[0] == pair[1]),
        "{grouped:?}"
    );
}

/// **A grouped line reads as a person reads it**: for each shape — one key position, two, a
/// root and one, a root and two, a `(no value)` part — each row is reached by the line its
/// printed cells fill, `<k-th>` the k-th part on the row, the root counted.
#[test]
fn each_indented_line_fills_from_the_printed_parts() {
    let mut library = planted();
    for (grouping, by_root) in [
        (vec!["source.mtime"], false),
        (vec!["source.mtime,size"], false),
        (vec!["source.mtime"], true),
        (vec!["source.mtime,size"], true),
        (vec!["content.V"], false),
    ] {
        check_grouped(&mut library, "source.ext", &grouping, by_root);
    }
}

/// A grouped row coarse at its group's value and at its part is reached by its line, exact at
/// both terms; so is the coarse group itself.
#[test]
fn a_row_coarse_at_two_terms_is_reached_by_its_line() {
    let mut library = Library::new();
    for (main, part) in [
        ("2024", "2024-06-15"),
        ("2024", "2024-06-15"),
        ("2024-03-01", "2024-06-15T10:00"),
        ("2024", "2024-06-15T10:00"),
    ] {
        let id = library.time("content.T", main);
        crate::core::testing::insert_time_fact(&library.conn, id, "content.U", part);
    }
    check_grouped(&mut library, "content.T", &["content.U"], false);
}

/// **A key of mixed type's rows are refused when pasted**, never claimed to read back: its rows
/// carry no filter, and each, pasted as `<header>=<row>`, is refused by the reading every filter
/// goes through.
#[test]
fn a_mixed_key_s_rows_are_refused_when_pasted() {
    let mut library = Library::new();
    library.text("content.Mixed", "a");
    library.number("content.Mixed", 5.0);
    library.time("content.Mixed", "2024");
    let report = library.distribution("content.Mixed", 0);
    assert_eq!(report.rows.len(), 3);
    for row in &report.rows {
        assert!(row.filter.is_none());
        assert!(library.refuses(&format!("{}={}", report.header, row.cell)));
    }
}

/// A value holding a single quote, pasted inside single shell quotes with each `'` written
/// `'\''`, reaches Canon as printed and selects its row.
#[test]
fn a_value_holding_a_single_quote_pastes_with_the_recipe() {
    let mut library = Library::new();
    let id = library.text("content.Genre", "Rock 'n' Roll");
    library.text("content.Genre", "Jazz");
    let report = library.distribution("content.Genre", 0);
    let row = report
        .rows
        .iter()
        .find(|row| row.cell.contains("Rock"))
        .unwrap();
    let filter = row.filter.as_ref().unwrap().text();
    let typed = format!("'{}'", filter.replace('\'', r"'\''"));
    assert_eq!(typed, r#"'content.Genre="Rock '\''n'\'' Roll"'"#);
    assert_eq!(
        library.selected(&[&posix_words(&typed)]),
        BTreeSet::from([id])
    );
}

/// What a POSIX shell hands a program for one word built of single-quoted parts and `\'`.
fn posix_words(typed: &str) -> String {
    let mut out = String::new();
    let mut chars = typed.chars();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                for inner in chars.by_ref() {
                    if inner == '\'' {
                        break;
                    }
                    out.push(inner);
                }
            }
            '\\' => out.extend(chars.next()),
            other => out.push(other),
        }
    }
    out
}
