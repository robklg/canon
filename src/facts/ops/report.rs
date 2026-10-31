//! Facts distribution computation.
//!
//! The `facts` report's four tables — one key's distribution, a grouped distribution, the
//! per-root table and the key list — as typed rows: each counted row with its cell, its count,
//! the filter it stands for (exact, or marked and stated as wider), and the lines under its
//! table. Every `--where` text a row or a line carries is spelled by the expression facility;
//! this module decides which rows a table has, their order, which are marked, and which lines it
//! prints. Marks are decided over every row of the selection, before `--limit` cuts any.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::Result;

use crate::core::domain::fact::KeyTypes;
use crate::core::domain::source::Source;
use crate::core::domain::time::zone::UserZone;
use crate::core::repo::{self, Connection};
use crate::expr::{
    contained, count_values_by_key, fetch_row_values, holds_control, mixed_type_statement,
    mixed_type_way_out, pictured, printed_key, source_row_value, Grain, Key, KeyKind, Listing,
    Modifier, ParsedFactKey, RowFilter, RowIdentity, RowValue, Slot, SourceAttributes, SourceFact,
};

/// What a row, a group or a part with no value prints.
const NO_VALUE: &str = "(no value)";

/// The placeholder a line writes for a plain row's value, or the one grouping position's.
const ROW: &str = "<row>";

/// The placeholder a grouped table's lines write for the group's value.
const VALUE: &str = "<value>";

// ============================================================================
// The report's keys
// ============================================================================

/// The keys a report counts by: its main key, and each grouping position in print order.
pub struct ReportKeys {
    pub main: Option<ParsedFactKey>,
    pub grouping: Vec<Position>,
}

/// One grouping position: the source's root (`--by-root`), or a key.
pub enum Position {
    Root,
    Key(ParsedFactKey),
}

/// **The one default owner**: every key a report counts by — the main key and each grouping
/// key — gets its default here, once, before any table is computed, so every table counts the
/// same expression and every header names it. `--by-root` is the first position.
pub fn report_keys(
    main: Option<ParsedFactKey>,
    grouping: Vec<ParsedFactKey>,
    by_root: bool,
) -> Result<ReportKeys> {
    let main = main.map(default_expression).transpose()?;
    let mut positions: Vec<Position> = Vec::new();
    if by_root {
        positions.push(Position::Root);
    }
    for key in grouping {
        positions.push(Position::Key(default_expression(key)?));
    }
    Ok(ReportKeys {
        main,
        grouping: positions,
    })
}

/// **A report's default, as an expression**: a modification time with no accessor or modifier
/// counts by year (`|year`), a size by range (`|bucket`) — read by the key the name reads as, so
/// `mtime`, `size` and a key alias standing for either take it too. Any other key, and any key
/// written with an accessor or a modifier, counts as written.
fn default_expression(key: ParsedFactKey) -> Result<ParsedFactKey> {
    if key.has_transforms() {
        return Ok(key);
    }
    match key.key.as_ref().and_then(Key::as_source_fact) {
        Some(SourceFact::Mtime) => key.with_modifier(Modifier::Year),
        Some(SourceFact::Size) => key.with_modifier(Modifier::Bucket),
        Some(
            SourceFact::Ext
            | SourceFact::Path
            | SourceFact::RelPath
            | SourceFact::Root
            | SourceFact::Id
            | SourceFact::Device
            | SourceFact::Inode,
        )
        | None => Ok(key),
    }
}

// ============================================================================
// Result types
// ============================================================================

/// A mark beside a row whose own value, pasted, does not select exactly what it counted: a
/// coarse time beside finer ones in its period (`+`), or a value holding a control character
/// (`!`). Each has its line under the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    Coarse,
    Unshowable,
}

/// A counted row: its cell as printed, its count, the filter it stands for — none on a key of
/// mixed type — its mark, and on a key of mixed type its value's type.
///
/// Every counted row carries its filter, here and on [`SubRow`], [`Group`] and [`RootRow`]. The
/// screen prints the lines under a table instead, so the binary reads none of them; the
/// read-back round trip's verifier reads every one.
pub struct Row {
    pub cell: String,
    pub count: i64,
    #[allow(dead_code)]
    pub filter: Option<RowFilter>,
    pub mark: Option<Mark>,
    pub type_word: Option<&'static str>,
}

/// What a header says its rows are: their type, and the key's kind where it has one.
pub struct Annotation {
    pub rows: &'static str,
    pub kind: Option<KeyKind>,
}

/// A line under a table: what is on the row, and the `--where` text that asks for it — or, for
/// a line no pasted text can follow, the sentence alone. A key holding a control character is
/// written as it is; the screen shows it in control pictures.
pub struct Line {
    pub label: String,
    pub filter: Option<String>,
}

/// One key's distribution.
pub struct Distribution {
    /// The expression counted, as `--where` reads it; the screen shows a control character in
    /// it as its picture.
    pub header: String,
    pub annotation: Annotation,
    /// The statement that the key is of mixed type, and its way out; empty otherwise.
    pub mixed: Vec<String>,
    /// The value rows `--limit` keeps, in order.
    pub rows: Vec<Row>,
    /// The sources with no value, printed last whatever the limit.
    pub no_value: Option<Row>,
    pub lines: Vec<Line>,
    pub shown: usize,
    pub total_values: usize,
    pub total_sources: usize,
}

/// One printed part of a grouped row or group: its cell, and on a key of mixed type its value's
/// type, which the screen writes after the cell.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Part {
    pub cell: String,
    pub type_word: Option<&'static str>,
}

/// A grouped row under its group: each grouping position's part, in print order.
pub struct SubRow {
    pub parts: Vec<Part>,
    pub count: i64,
    #[allow(dead_code)]
    pub filter: Option<RowFilter>,
    pub mark: Option<Mark>,
}

/// One main value — or the sources with none — and its rows by the grouping positions.
pub struct Group {
    pub part: Part,
    pub total: i64,
    #[allow(dead_code)]
    pub filter: Option<RowFilter>,
    pub mark: Option<Mark>,
    pub rows: Vec<SubRow>,
}

/// A grouped distribution.
pub struct Grouped {
    /// The expressions counted, as `--where` reads them, under `(grouped by …)`.
    pub header: String,
    pub mixed: Vec<String>,
    /// The value groups `--limit` keeps, in order.
    pub groups: Vec<Group>,
    /// The sources with no main value, printed last whatever the limit.
    pub no_value_group: Option<Group>,
    pub lines: Vec<Line>,
    pub shown: usize,
    pub total_groups: usize,
    pub total_sources: usize,
}

/// One root's row in the per-root table.
pub struct RootRow {
    pub cell: String,
    pub count: usize,
    #[allow(dead_code)]
    pub filter: RowFilter,
}

/// The per-root table.
pub struct RootTable {
    pub rows: Vec<RootRow>,
    pub lines: Vec<Line>,
    pub total_sources: usize,
}

/// Information about a single fact key (for all-keys enumeration).
pub struct KeyInfo {
    /// The key as stored, which the mixed-type lines name.
    pub key: String,
    /// The key as printed, control pictures in place.
    pub cell: String,
    /// Whether the key holds a control character, so it cannot be pasted as printed.
    pub mark: bool,
    pub count: i64,
    pub kind: KeyKind,
    /// The key's type, read from all its facts wherever they sit — never from the sources
    /// counted here. A source fact's or a built-in key's is its own.
    pub types: KeyTypes,
}

/// Result of enumerating all available fact keys.
pub struct AllKeysResult {
    pub keys: Vec<KeyInfo>,
    pub lines: Vec<Line>,
    pub total_sources: usize,
}

// ============================================================================
// The per-root table
// ============================================================================

/// **The per-root table**: each root's count of the selection, by count then id, its full path
/// and its filter, `root_id=<id>` — a root's own id, never a source's.
pub fn compute_root_distribution(sources: &[Source]) -> RootTable {
    let mut counts: BTreeMap<i64, (&str, usize)> = BTreeMap::new();
    for source in sources {
        counts
            .entry(source.root_id)
            .or_insert((source.root_path.as_str(), 0))
            .1 += 1;
    }
    let mut rows: Vec<RootRow> = counts
        .into_iter()
        .map(|(id, (path, count))| RootRow {
            cell: root_cell(id, path),
            count,
            filter: RowFilter::root(Some(id)),
        })
        .collect();
    // A stable sort over the ids' order: equal counts stay by id.
    rows.sort_by(|a, b| b.count.cmp(&a.count));
    RootTable {
        lines: if rows.is_empty() {
            Vec::new()
        } else {
            vec![root_line()]
        },
        rows,
        total_sources: sources.len(),
    }
}

/// A root as a row or a part prints it: its id, two spaces, its full path, control pictures in
/// place.
fn root_cell(id: i64, path: &str) -> String {
    format!("id:{id}  {}", pictured(path))
}

/// The line under every table that prints root rows.
fn root_line() -> Line {
    Line {
        label: "id:<N> (not id=, which names a source)".to_string(),
        filter: Some(RowFilter::root(None).text().to_string()),
    }
}

// ============================================================================
// The key list
// ============================================================================

/// **The key list**: the listed source facts and built-in keys, then every stored key the
/// selection holds, each counted as `<key>?` selects — a computed key where the resolver has a
/// value (every source, but for a save time Canon cannot read), a stored key where the source
/// has its one value ([`count_values_by_key`]). Each key is printed by the printing rule, and one
/// holding a control character is marked, with one line naming what stands for it. Each key's
/// type is the key's own, from all its facts: a key of mixed type lists as mixed, whatever the
/// scope holds.
pub fn compute_all_keys(
    conn: &mut Connection,
    source_ids: &[i64],
    show_hidden: bool,
) -> Result<AllKeysResult> {
    let total_sources = source_ids.len();
    if source_ids.is_empty() {
        return Ok(AllKeysResult {
            keys: Vec::new(),
            lines: Vec::new(),
            total_sources,
        });
    }

    let mut keys: Vec<KeyInfo> = Vec::new();
    let sources: Vec<SourceAttributes> = repo::source::batch_fetch_by_ids(conn, source_ids)?
        .values()
        .map(SourceAttributes::from)
        .collect();

    for (key, known, _) in Key::all_typed() {
        match key.listing() {
            Listing::NotListed => continue,
            Listing::Hidden if !show_hidden => continue,
            Listing::Hidden | Listing::Shown => {}
        }
        let count = sources
            .iter()
            .filter(|source| source_row_value(source, &key).is_some())
            .count();
        keys.push(key_info(key.name(), count, key.kind(), KeyTypes::of(known)));
    }

    // The count is the scope's, the type the key's. A key whose facts were all removed between
    // the two reads has no type, and is not listed.
    let stored = count_values_by_key(conn, source_ids)?;
    let listed: Vec<&str> = stored.iter().map(|(key, _)| key.as_str()).collect();
    let mut types = repo::fact::key_types(conn, &listed)?;
    for (key, count) in stored {
        let Some(types) = types.remove(&key) else {
            continue;
        };
        keys.push(key_info(&key, count, KeyKind::ContentFact, types));
    }

    let lines = control_key_line(&keys).into_iter().collect();
    Ok(AllKeysResult {
        keys,
        lines,
        total_sources,
    })
}

fn key_info(key: &str, count: usize, kind: KeyKind, types: KeyTypes) -> KeyInfo {
    KeyInfo {
        key: key.to_string(),
        cell: pictured(&printed_key(key)),
        mark: holds_control(key),
        count: count as i64,
        kind,
        types,
    }
}

/// The line under a key list holding a key marked `!`: what each control character it holds is
/// shown as, so the key cannot be pasted as printed. None where no key is marked.
fn control_key_line(keys: &[KeyInfo]) -> Option<Line> {
    key_control_line(
        keys.iter()
            .filter(|info| info.mark)
            .map(|info| info.key.as_str()),
        "Key marked !",
    )
}

/// **The line for keys holding a control character**: what each control character the keys
/// hold is shown as, so a key cannot be pasted as printed — asked of the facility's own reading
/// of a control character. None where no key holds one.
fn key_control_line<'k>(keys: impl Iterator<Item = &'k str>, label: &str) -> Option<Line> {
    let pictures: BTreeSet<String> = keys
        .flat_map(|key| key.chars())
        .map(|c| c.to_string())
        .filter(|c| holds_control(c))
        .map(|c| pictured(&c))
        .collect();
    if pictures.is_empty() {
        return None;
    }
    let pictures: Vec<String> = pictures.into_iter().collect();
    Some(Line {
        label: format!(
            "{label}: a control character stands where {} shows, so the key cannot be pasted as printed",
            pictures.join(" or ")
        ),
        filter: None,
    })
}

/// **The lines under a table**, and last, where a key in its header holds a control character,
/// the line saying the key cannot be pasted as printed.
fn closing_lines(keys: &[&ParsedFactKey], mut lines: Vec<Line>) -> Vec<Line> {
    let printed: Vec<String> = keys.iter().map(|key| key.printed()).collect();
    lines.extend(key_control_line(
        printed.iter().map(String::as_str),
        "Key in the header",
    ));
    lines
}

// ============================================================================
// One key's distribution
// ============================================================================

/// **One key's distribution**: every source's value for the key, through the facility's one
/// reader; one row per value as printed and typed ([`RowIdentity`]), by count and then the value's own order; `(no value)` for
/// the rest, last, whatever the limit. A row whose value is coarser than other values counted
/// in its period is marked `+`, and its filter is the exact one; a row holding a control
/// character is marked `!`, its filter wider. A key of mixed type prints its values with their
/// types, no filter, and the statement and way out above the table.
pub fn compute_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &ParsedFactKey,
    limit: usize,
    zone: &UserZone,
) -> Result<Distribution> {
    let fetched = repo::source::batch_fetch_by_ids(conn, source_ids)?;
    let sources: Vec<&Source> = source_ids.iter().filter_map(|id| fetched.get(id)).collect();
    let values = fetch_row_values(conn, &sources, &[key], zone)?;

    let mut tally: HashMap<RowIdentity, (RowValue, i64)> = HashMap::new();
    let mut valued: i64 = 0;
    for value in values
        .into_values()
        .filter_map(|row| row.into_iter().next().flatten())
    {
        valued += 1;
        tally.entry(value.identity()).or_insert((value, 0)).1 += 1;
    }
    let mut distinct: Vec<(RowValue, i64)> = tally.into_values().collect();
    distinct.sort_by(|(a, a_count), (b, b_count)| b_count.cmp(a_count).then_with(|| a.cmp(b)));

    let refs: Vec<&RowValue> = distinct.iter().map(|(value, _)| value).collect();
    let coarse = contained(&refs);
    let total_values = distinct.len();
    let shown = shown_of(total_values, limit);

    let mut rows = Vec::with_capacity(shown);
    let mut grains: BTreeSet<Grain> = BTreeSet::new();
    let mut unshowable: Vec<Line> = Vec::new();
    for ((value, count), within) in distinct.iter().zip(&coarse).take(shown) {
        let printed = key.row(value);
        let grain = value.grain().filter(|_| !within.is_empty());
        let (filter, mark) = marked(key, value, printed.filter, grain);
        match mark {
            Some(Mark::Coarse) => grains.extend(grain),
            Some(Mark::Unshowable) => unshowable.push(unshowable_line(&printed.cell, &filter)),
            None => {}
        }
        rows.push(Row {
            cell: printed.cell,
            count: *count,
            filter,
            mark,
            type_word: value_type(key, value),
        });
    }

    let mut lines: Vec<Line> = grains
        .into_iter()
        .map(|grain| Line {
            label: format!("Each {} row marked +", grain.word()),
            filter: Some(
                key.exactly(Slot::Placeholder(ROW), grain)
                    .text()
                    .to_string(),
            ),
        })
        .collect();
    lines.extend(unshowable);

    let without = sources.len() as i64 - valued;
    let no_value = (without > 0).then(|| {
        lines.push(Line {
            label: NO_VALUE.to_string(),
            filter: Some(key.no_value().text().to_string()),
        });
        Row {
            cell: NO_VALUE.to_string(),
            count: without,
            filter: Some(key.no_value()),
            mark: None,
            type_word: None,
        }
    });

    Ok(Distribution {
        header: key.printed(),
        annotation: annotation(key),
        mixed: mixed_lines(&[key]),
        rows,
        no_value,
        lines: closing_lines(&[key], lines),
        shown,
        total_values,
        total_sources: sources.len(),
    })
}

/// How many of `total` rows a `--limit` of `limit` keeps; 0 keeps all.
fn shown_of(total: usize, limit: usize) -> usize {
    if limit > 0 {
        total.min(limit)
    } else {
        total
    }
}

/// A value row's filter and mark: none on a key of mixed type; `!` where the filter is wider;
/// `+` where the value prints at `grain` beside finer values in its period, its filter then
/// the exact one.
fn marked(
    key: &ParsedFactKey,
    value: &RowValue,
    filter: Option<RowFilter>,
    grain: Option<Grain>,
) -> (Option<RowFilter>, Option<Mark>) {
    match (filter, grain) {
        (None, _) => (None, None),
        (Some(filter), _) if !filter.is_exact() => (Some(filter), Some(Mark::Unshowable)),
        (Some(_), Some(grain)) => (
            Some(key.exactly(Slot::Value(value), grain)),
            Some(Mark::Coarse),
        ),
        (Some(filter), None) => (Some(filter), None),
    }
}

/// The line under a row marked `!`.
fn unshowable_line(cell: &str, filter: &Option<RowFilter>) -> Line {
    Line {
        label: format!("Row ! {cell} (may select more)"),
        filter: filter.as_ref().map(|filter| filter.text().to_string()),
    }
}

/// What a header says its rows are.
fn annotation(key: &ParsedFactKey) -> Annotation {
    let kind = match key.key.as_ref().map(Key::kind) {
        Some(KeyKind::ContentFact) | None => None,
        Some(kind @ (KeyKind::SourceFact | KeyKind::BuiltinKey)) => Some(kind),
    };
    Annotation {
        rows: key.rows_word(),
        kind,
    }
}

/// The statement that keys a report counts by are of mixed type, and the way out; each key once.
fn mixed_lines(keys: &[&ParsedFactKey]) -> Vec<String> {
    let mut mixed: Vec<(&str, &KeyTypes)> = Vec::new();
    for key in keys {
        if let Some(types) = key.mixed_types() {
            if !mixed.iter().any(|(named, _)| *named == key.base_key) {
                mixed.push((&key.base_key, types));
            }
        }
    }
    let mut lines = mixed_type_statement(&mixed);
    lines.extend(mixed_type_way_out(&mixed));
    lines
}

// ============================================================================
// A grouped distribution
// ============================================================================

/// One source's part in one grouping position, as a group's rows tell them apart.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PartId {
    Value(RowIdentity),
    Root(i64),
    NoValue,
}

/// A sub-row's identity: its main value (none in the `(no value)` group) and its parts.
type SubRowId = (Option<RowIdentity>, Vec<PartId>);

/// One group's rows as counted: each row's parts and its count.
type GroupRows<'a> = Vec<(&'a Vec<PartId>, i64)>;

/// What a grouped distribution read: each distinct main value and each position's distinct
/// values, every root met, and the count of each sub-row.
struct Tally {
    main: HashMap<RowIdentity, RowValue>,
    parts: Vec<HashMap<RowIdentity, RowValue>>,
    roots: HashMap<i64, String>,
    counts: HashMap<SubRowId, i64>,
}

/// For a set of distinct values, the values each selects besides itself.
type Within = HashMap<RowIdentity, HashSet<RowIdentity>>;

fn within(values: &HashMap<RowIdentity, RowValue>) -> Within {
    let listed: Vec<&RowValue> = values.values().collect();
    contained(&listed)
        .into_iter()
        .enumerate()
        .map(|(i, inside)| {
            let selected = inside.into_iter().map(|j| listed[j].identity()).collect();
            (listed[i].identity(), selected)
        })
        .collect()
}

/// **A grouped distribution** (`--group-by`, `--by-root` with a key): each main value's group,
/// and under it a row per combination of the grouping positions' values — a root, a key's
/// value, or `(no value)` — counted, ordered, marked and given its filter, the conjunction of its
/// group's term and each position's. The sources with no main value form the `(no value)` group,
/// with rows of their own, last whatever the limit; `--limit` keeps that many value groups. Marks
/// are decided over every group, before the limit.
pub fn compute_grouped_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    main: &ParsedFactKey,
    positions: &[Position],
    limit: usize,
    zone: &UserZone,
) -> Result<Grouped> {
    let fetched = repo::source::batch_fetch_by_ids(conn, source_ids)?;
    let sources: Vec<&Source> = source_ids.iter().filter_map(|id| fetched.get(id)).collect();
    let mut keys: Vec<&ParsedFactKey> = vec![main];
    keys.extend(positions.iter().filter_map(|position| match position {
        Position::Key(key) => Some(key),
        Position::Root => None,
    }));
    let mut values = fetch_row_values(conn, &sources, &keys, zone)?;

    let mut tally = Tally {
        main: HashMap::new(),
        parts: positions.iter().map(|_| HashMap::new()).collect(),
        roots: HashMap::new(),
        counts: HashMap::new(),
    };
    for source in &sources {
        let mut read = values.remove(&source.id).unwrap_or_default().into_iter();
        let main_id = read.next().flatten().map(|value| {
            let identity = value.identity();
            tally.main.entry(identity.clone()).or_insert(value);
            identity
        });
        let mut parts = Vec::with_capacity(positions.len());
        for (at, position) in positions.iter().enumerate() {
            parts.push(match position {
                Position::Root => {
                    tally
                        .roots
                        .entry(source.root_id)
                        .or_insert_with(|| source.root_path.clone());
                    PartId::Root(source.root_id)
                }
                Position::Key(_) => match read.next().flatten() {
                    Some(value) => {
                        let identity = value.identity();
                        tally.parts[at].entry(identity.clone()).or_insert(value);
                        PartId::Value(identity)
                    }
                    None => PartId::NoValue,
                },
            });
        }
        *tally.counts.entry((main_id, parts)).or_default() += 1;
    }

    let main_within = within(&tally.main);
    let parts_within: Vec<Within> = tally.parts.iter().map(within).collect();
    let grouping = Grouping {
        main,
        positions,
        tally: &tally,
        main_within: &main_within,
        parts_within: &parts_within,
    };

    // The groups, by total and then their value's order; `(no value)` apart.
    let mut by_group: HashMap<Option<RowIdentity>, GroupRows> = HashMap::new();
    for ((main_id, parts), count) in &tally.counts {
        by_group
            .entry(main_id.clone())
            .or_default()
            .push((parts, *count));
    }
    let no_value_rows = by_group.remove(&None);
    let mut groups: Vec<(&RowValue, GroupRows)> = by_group
        .into_iter()
        .filter_map(|(main_id, rows)| Some((&tally.main[&main_id?], rows)))
        .collect();
    groups.sort_by(|(a, a_rows), (b, b_rows)| {
        total(b_rows).cmp(&total(a_rows)).then_with(|| a.cmp(b))
    });
    let total_groups = groups.len();
    let shown = shown_of(total_groups, limit);

    let mut lines = LineSet::default();
    let groups: Vec<Group> = groups
        .into_iter()
        .take(shown)
        .map(|(value, rows)| grouping.group(Some(value), rows, &mut lines))
        .collect();
    let no_value_group = no_value_rows.map(|rows| grouping.group(None, rows, &mut lines));

    Ok(Grouped {
        header: format!(
            "{} (grouped by {})",
            main.printed(),
            positions
                .iter()
                .map(position_name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        mixed: mixed_lines(&keys),
        groups,
        no_value_group,
        lines: closing_lines(&keys, grouping.lines(lines)),
        shown,
        total_groups,
        total_sources: sources.len(),
    })
}

fn total(rows: &[(&Vec<PartId>, i64)]) -> i64 {
    rows.iter().map(|(_, count)| count).sum()
}

/// A grouping position as a grouped header names it: `root`, or the key as printed — a content
/// fact that would print as that word printing its full name.
fn position_name(position: &Position) -> String {
    match position {
        Position::Root => "root".to_string(),
        Position::Key(key) => {
            let printed = key.printed();
            if printed == "root" {
                printed_key(&key.base_key)
            } else {
                printed
            }
        }
    }
}

/// A grouped table's part for a value: its cell, and on a key of mixed type its type in the
/// words the plain table's Type column uses, since values of two types can print alike.
fn typed_part(key: &ParsedFactKey, value: &RowValue, cell: String) -> Part {
    Part {
        cell,
        type_word: value_type(key, value),
    }
}

/// **What a table says a value's type is**: on a key of mixed type, the value's type in the
/// Type column's words; on any other key nothing, the header saying it once.
fn value_type(key: &ParsedFactKey, value: &RowValue) -> Option<&'static str> {
    key.mixed_types().map(|_| value.type_word())
}

/// A part with no type of its own: a root, or `(no value)`.
fn untyped_part(cell: String) -> Part {
    Part {
        cell,
        type_word: None,
    }
}

/// The ordinal placeholder for the `n`-th printed part (from 1): `<1st>`, `<2nd>`, `<3rd>`,
/// `<4th>` …
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

/// What a grouped table reads its groups and rows from.
struct Grouping<'a> {
    main: &'a ParsedFactKey,
    positions: &'a [Position],
    tally: &'a Tally,
    main_within: &'a Within,
    parts_within: &'a [Within],
}

/// What stands in one position of a line's conjunction.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Term {
    /// The position's value, by its placeholder.
    Value,
    /// A coarse value, exactly, at its grain.
    Coarse(Grain),
    /// `(no value)`: `NOT <key>?`.
    Missing,
}

/// The lines a grouped table's printed rows call for, gathered as they print: each distinct
/// conjunction shape, by the terms in each position — the main position first.
#[derive(Default)]
struct LineSet {
    /// Value groups' rows, by their terms.
    rows: BTreeSet<Vec<Term>>,
    /// The `(no value)` group's rows, by their terms.
    under_no_value: BTreeSet<Vec<Term>>,
    /// The grains of marked value groups.
    groups: BTreeSet<Grain>,
    /// Each printed row marked `!`, and its line.
    unshowable: Vec<Line>,
    /// Whether the `(no value)` group prints.
    no_value_group: bool,
    roots: bool,
}

impl Grouping<'_> {
    /// One group and its rows, in order, each marked and given its filter; the lines it calls
    /// for added to `lines`. A row with a value comes before one with `(no value)` at the first
    /// part where only one of them has a value, whatever their counts; then by count, then by
    /// their parts.
    fn group(&self, value: Option<&RowValue>, mut rows: GroupRows, lines: &mut LineSet) -> Group {
        fn absent(parts: &[PartId]) -> impl Iterator<Item = bool> + '_ {
            parts.iter().map(|part| matches!(part, PartId::NoValue))
        }
        rows.sort_by(|(a, a_count), (b, b_count)| {
            absent(a)
                .cmp(absent(b))
                .then_with(|| b_count.cmp(a_count))
                .then_with(|| self.compare_parts(a, b))
        });
        let main_id = value.map(RowValue::identity);
        let main_grain = value.and_then(|value| self.coarse_grain(value, self.main_within));

        let (part, filter, mark) = match value {
            Some(value) => {
                let printed = self.main.row(value);
                let (filter, mark) = marked(self.main, value, printed.filter, main_grain);
                match mark {
                    Some(Mark::Coarse) => lines.groups.extend(main_grain),
                    Some(Mark::Unshowable) => lines
                        .unshowable
                        .push(unshowable_line(&printed.cell, &filter)),
                    None => {}
                }
                (typed_part(self.main, value, printed.cell), filter, mark)
            }
            None => {
                lines.no_value_group = true;
                (
                    untyped_part(NO_VALUE.to_string()),
                    Some(self.main.no_value()),
                    None,
                )
            }
        };

        let sub_rows = rows
            .iter()
            .map(|(parts, count)| self.sub_row(&part.cell, value, &main_id, parts, *count, lines))
            .collect();
        Group {
            part,
            total: total(&rows),
            filter,
            mark,
            rows: sub_rows,
        }
    }

    /// A row under a group: its cells, its filter and its mark, and the line shape it calls for.
    fn sub_row(
        &self,
        group_cell: &str,
        value: Option<&RowValue>,
        main_id: &Option<RowIdentity>,
        parts: &[PartId],
        count: i64,
        lines: &mut LineSet,
    ) -> SubRow {
        let main_grain = value.and_then(|value| self.coarse_grain(value, self.main_within));
        let coarse = self.is_coarse(main_id, parts);
        let mut terms = vec![match (value, coarse.then_some(main_grain).flatten()) {
            (None, _) => Term::Missing,
            (Some(_), Some(grain)) => Term::Coarse(grain),
            (Some(_), None) => Term::Value,
        }];
        let mut filters = vec![match value {
            Some(value) => match terms[0] {
                Term::Coarse(grain) => Some(self.main.exactly(Slot::Value(value), grain)),
                Term::Value | Term::Missing => self.main.row(value).filter,
            },
            None => Some(self.main.no_value()),
        }];
        let mut cells: Vec<Part> = Vec::with_capacity(parts.len());
        for (at, (position, part)) in self.positions.iter().zip(parts).enumerate() {
            match (position, part) {
                (Position::Root, PartId::Root(id)) => {
                    lines.roots = true;
                    cells.push(untyped_part(root_cell(*id, &self.tally.roots[id])));
                    filters.push(Some(RowFilter::root(Some(*id))));
                    terms.push(Term::Value);
                }
                (Position::Key(key), PartId::Value(printed)) => {
                    let value = &self.tally.parts[at][printed];
                    let row = key.row(value);
                    cells.push(typed_part(key, value, row.cell));
                    let grain = coarse
                        .then(|| self.coarse_grain(value, &self.parts_within[at]))
                        .flatten();
                    match grain {
                        Some(grain) => {
                            filters.push(Some(key.exactly(Slot::Value(value), grain)));
                            terms.push(Term::Coarse(grain));
                        }
                        None => {
                            filters.push(row.filter);
                            terms.push(Term::Value);
                        }
                    }
                }
                (Position::Key(key), PartId::NoValue) => {
                    cells.push(untyped_part(NO_VALUE.to_string()));
                    filters.push(Some(key.no_value()));
                    terms.push(Term::Missing);
                }
                (Position::Root, PartId::Value(_) | PartId::NoValue)
                | (Position::Key(_), PartId::Root(_)) => {
                    unreachable!("a position's part is read by the position")
                }
            }
        }
        let filter: Option<Vec<RowFilter>> = filters.into_iter().collect();
        let filter = filter.map(RowFilter::all);
        let mark = match &filter {
            None => None,
            Some(filter) if !filter.is_exact() => Some(Mark::Unshowable),
            Some(_) if coarse => Some(Mark::Coarse),
            Some(_) => None,
        };
        if mark == Some(Mark::Unshowable) {
            let printed: Vec<&str> = cells.iter().map(|part| part.cell.as_str()).collect();
            let cell = format!("{group_cell} / {}", printed.join(" / "));
            lines.unshowable.push(unshowable_line(&cell, &filter));
        }
        match value {
            Some(_) => lines.rows.insert(terms),
            None => lines.under_no_value.insert(terms),
        };
        SubRow {
            parts: cells,
            count,
            filter,
            mark,
        }
    }

    /// The grain a value prints at, where it selects another value of its position.
    fn coarse_grain(&self, value: &RowValue, within: &Within) -> Option<Grain> {
        value.grain().filter(|_| {
            within
                .get(&value.identity())
                .is_some_and(|set| !set.is_empty())
        })
    }

    /// **Whether a row selects another row of the table**: another row whose main value its main
    /// value selects and whose every part its part selects — the same value, a finer value in a
    /// coarse value's period, `(no value)` beside `(no value)`, the same root. Each combination of
    /// what each term selects is looked up, so only a row with a coarse term looks past itself.
    fn is_coarse(&self, main_id: &Option<RowIdentity>, parts: &[PartId]) -> bool {
        let main_options: Vec<Option<RowIdentity>> = std::iter::once(main_id.clone())
            .chain(
                main_id
                    .as_ref()
                    .and_then(|id| self.main_within.get(id))
                    .into_iter()
                    .flatten()
                    .map(|inside| Some(inside.clone())),
            )
            .collect();
        let part_options: Vec<Vec<PartId>> = parts
            .iter()
            .enumerate()
            .map(|(at, part)| {
                let mut options = vec![part.clone()];
                if let PartId::Value(printed) = part {
                    if let Some(inside) = self.parts_within[at].get(printed) {
                        options.extend(inside.iter().cloned().map(PartId::Value));
                    }
                }
                options
            })
            .collect();
        if main_options.len() == 1 && part_options.iter().all(|options| options.len() == 1) {
            return false;
        }
        let mut chosen: Vec<PartId> = Vec::with_capacity(parts.len());
        main_options.iter().any(|main_option| {
            self.any_other(main_id, parts, main_option, &part_options, &mut chosen)
        })
    }

    /// Whether some combination of the remaining positions' options, after `chosen`, names a
    /// row of the table other than the row itself.
    fn any_other(
        &self,
        main_id: &Option<RowIdentity>,
        parts: &[PartId],
        main_option: &Option<RowIdentity>,
        part_options: &[Vec<PartId>],
        chosen: &mut Vec<PartId>,
    ) -> bool {
        let Some((options, rest)) = part_options.split_first() else {
            let same = main_option == main_id && chosen.as_slice() == parts;
            return !same
                && self
                    .tally
                    .counts
                    .contains_key(&(main_option.clone(), chosen.clone()));
        };
        options.iter().any(|option| {
            chosen.push(option.clone());
            let found = self.any_other(main_id, parts, main_option, rest, chosen);
            chosen.pop();
            found
        })
    }

    /// Two rows' parts in print order, `(no value)` in the same positions of both: a value by the
    /// value's order, a root by its id.
    fn compare_parts(&self, a: &[PartId], b: &[PartId]) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        for (at, (a, b)) in a.iter().zip(b).enumerate() {
            let order = match (a, b) {
                (PartId::Value(a), PartId::Value(b)) => {
                    self.tally.parts[at][a].cmp(&self.tally.parts[at][b])
                }
                (PartId::Root(a), PartId::Root(b)) => a.cmp(b),
                (PartId::NoValue, PartId::NoValue) => Ordering::Equal,
                (PartId::NoValue, PartId::Value(_) | PartId::Root(_))
                | (PartId::Value(_) | PartId::Root(_), PartId::NoValue) => {
                    unreachable!("which parts have no value decides before the parts' order")
                }
                (PartId::Value(_), PartId::Root(_)) | (PartId::Root(_), PartId::Value(_)) => {
                    unreachable!("a position's parts are of one kind")
                }
            };
            if order != Ordering::Equal {
                return order;
            }
        }
        Ordering::Equal
    }

    /// The lines under a grouped table, from the shapes its printed rows call for: the
    /// conjunctions — plain, with `(no value)` parts, under the `(no value)` group — then each
    /// marked group's grain and each marked row's shape, each row marked `!`, the `(no value)`
    /// group's own line where it prints, and the root line.
    /// A conjunction that would hold a value of a key of mixed type is not printed.
    fn lines(&self, set: LineSet) -> Vec<Line> {
        let mut lines = Vec::new();
        let plain = |terms: &Vec<Term>| {
            terms
                .iter()
                .all(|term| matches!(term, Term::Value | Term::Missing))
        };
        let missing = |terms: &Vec<Term>| terms[1..].contains(&Term::Missing);
        for (under, shapes) in [(false, &set.rows), (true, &set.under_no_value)] {
            for terms in shapes
                .iter()
                .filter(|terms| plain(terms) && !missing(terms))
            {
                let label = if under {
                    "Each indented row under (no value)"
                } else {
                    "Each indented row"
                };
                lines.extend(self.line(label.to_string(), terms));
            }
            for terms in shapes.iter().filter(|terms| plain(terms) && missing(terms)) {
                let label = if under {
                    "Each indented (no value) row under (no value)"
                } else {
                    "Each indented (no value) row"
                };
                lines.extend(self.line(label.to_string(), terms));
            }
        }
        for grain in &set.groups {
            lines.push(Line {
                label: format!("Each {} group marked +", grain.word()),
                filter: Some(
                    self.main
                        .exactly(Slot::Placeholder(VALUE), *grain)
                        .text()
                        .to_string(),
                ),
            });
        }
        for shape in [&set.rows, &set.under_no_value] {
            for terms in shape.iter().filter(|terms| !plain(terms)) {
                let grains: Vec<&str> = terms
                    .iter()
                    .filter_map(|term| match term {
                        Term::Coarse(grain) => Some(grain.word()),
                        Term::Value | Term::Missing => None,
                    })
                    .collect();
                let under = if terms[0] == Term::Missing {
                    " under (no value)"
                } else {
                    ""
                };
                let grains = match grains.split_last() {
                    Some((last, [])) => last.to_string(),
                    Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
                    None => unreachable!("a marked shape holds a coarse term"),
                };
                let label = format!("Each indented {grains} row marked +{under}");
                lines.extend(self.line(label, terms));
            }
        }
        lines.extend(set.unshowable);
        if set.no_value_group {
            lines.push(Line {
                label: NO_VALUE.to_string(),
                filter: Some(self.main.no_value().text().to_string()),
            });
        }
        if set.roots {
            lines.push(root_line());
        }
        lines
    }

    /// One conjunction line: `label: <main term> AND <each position's term>`, each value by its
    /// placeholder — `<value>` for the group's, `<row>` for the one position's, or each position's
    /// ordinal among the printed parts, a root counted; none where a value term is of a key of
    /// mixed type.
    fn line(&self, label: String, terms: &[Term]) -> Option<Line> {
        let ordinals = self.positions.len() > 1;
        let mut parts = Vec::with_capacity(terms.len());
        let keys = std::iter::once(Some(self.main)).chain(self.positions.iter().map(|position| {
            match position {
                Position::Key(key) => Some(key),
                Position::Root => None,
            }
        }));
        for (at, (key, term)) in keys.zip(terms).enumerate() {
            let placeholder = match (at, ordinals) {
                (0, _) => VALUE.to_string(),
                (_, false) => ROW.to_string(),
                (n, true) => ordinal(n),
            };
            parts.push(match (key, term) {
                (None, _) => RowFilter::root(None),
                (Some(key), Term::Missing) => key.no_value(),
                (Some(key), _) if key.mixed_types().is_some() => return None,
                (Some(key), Term::Value) => key.equals(Slot::Placeholder(&placeholder)),
                (Some(key), Term::Coarse(grain)) => {
                    key.exactly(Slot::Placeholder(&placeholder), *grain)
                }
            });
        }
        Some(Line {
            label,
            filter: Some(RowFilter::all(parts).text().to_string()),
        })
    }
}
