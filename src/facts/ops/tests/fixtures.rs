//! The library the report's tests read: a test database, sources planted with the values a
//! test names, a report read as `canon facts` reads it, and a filter run as `canon ls` runs it
//! over the same selection and zone.

use std::collections::{BTreeMap, BTreeSet};

use crate::core::domain::time::zone::UserZone;
use crate::core::domain::IncludeSet;
use crate::core::repo::Connection;
use crate::core::testing::{
    insert_fact, insert_number_fact, insert_object, insert_root, insert_source_with_metadata,
    insert_time_fact, setup_test_db,
};
use crate::expr::{
    read_filters, read_report, select_sources, ExpressionText, RolePolicy, SelectionParams,
};
use crate::facts::ops::report::{
    compute_distribution, compute_grouped_distribution, report_keys, Distribution, Grouped,
};

/// A library on a test database: its source roots, and the zone its reports and filters read
/// times on.
pub(super) struct Library {
    pub(super) conn: Connection,
    pub(super) root: i64,
    pub(super) zone: UserZone,
    next: usize,
    /// A canon home holding the aliases file the report's and the filters' texts read.
    aliases: Option<tempfile::TempDir>,
}

impl Library {
    /// One source root, `/photos`, read on UTC.
    pub(super) fn new() -> Library {
        Library::in_zone(UserZone::utc())
    }

    /// One source root, `/photos`, read on `zone`.
    pub(super) fn in_zone(zone: UserZone) -> Library {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        Library {
            conn,
            root,
            zone,
            next: 0,
            aliases: None,
        }
    }

    /// This library, its texts read with the aliases file `content`.
    pub(super) fn with_aliases(mut self, content: &str) -> Library {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(home.path().join("aliases.toml"), content).unwrap();
        self.aliases = Some(home);
        self
    }

    /// Texts as the command line gives them, with the library's aliases file where it has one.
    fn texts(&self, texts: &[&str]) -> ExpressionText {
        match &self.aliases {
            Some(home) => ExpressionText::given(
                texts.iter().map(|text| text.to_string()).collect(),
                home.path(),
            ),
            None => ExpressionText::plain(texts),
        }
    }

    /// Another source root.
    pub(super) fn add_root(&self, path: &str) -> i64 {
        insert_root(&self.conn, path, "source", false)
    }

    /// A source at `rel` in `root`, with its own content, `size` bytes, saved at `mtime`.
    pub(super) fn file_in(&mut self, root: i64, rel: &str, size: i64, mtime: i64) -> i64 {
        self.next += 1;
        let object = insert_object(&self.conn, &format!("hash{}", self.next), false);
        insert_source_with_metadata(&self.conn, root, rel, Some(object), size, mtime)
    }

    /// A source at `rel` in the first root, 1000 bytes, saved on 2024-01-01 at 00:00 UTC.
    pub(super) fn file(&mut self, rel: &str) -> i64 {
        let root = self.root;
        self.file_in(root, rel, 1000, 1_704_067_200)
    }

    /// A new source holding text `value` for `key`.
    pub(super) fn text(&mut self, key: &str, value: &str) -> i64 {
        let id = self.file(&format!("f{}.jpg", self.next + 1));
        insert_fact(&self.conn, id, key, value);
        id
    }

    /// A new source holding number `value` for `key`.
    pub(super) fn number(&mut self, key: &str, value: f64) -> i64 {
        let id = self.file(&format!("f{}.jpg", self.next + 1));
        insert_number_fact(&self.conn, id, key, value);
        id
    }

    /// A new source holding a time for `key`, written as `written`.
    pub(super) fn time(&mut self, key: &str, written: &str) -> i64 {
        let id = self.file(&format!("f{}.jpg", self.next + 1));
        insert_time_fact(&self.conn, id, key, written);
        id
    }

    /// Every source the report counts: the whole selection, as `canon ls` with no filter.
    pub(super) fn all(&mut self) -> Vec<i64> {
        self.selected(&[]).into_iter().collect()
    }

    /// **The sources `canon ls` lists** with these `--where` texts over the same selection, in
    /// the same zone.
    pub(super) fn selected(&mut self, filters: &[&str]) -> BTreeSet<i64> {
        let filters = read_filters(&self.conn, &self.texts(filters))
            .unwrap_or_else(|error| panic!("{filters:?} does not read: {error}"));
        let params = SelectionParams {
            scopes: Vec::new(),
            include: IncludeSet::default(),
            filters,
            role_policy: RolePolicy::SourceUnlessIncluded,
            clock: self.zone.clone(),
            notes: false,
        };
        select_sources(&mut self.conn, &params)
            .unwrap()
            .source_ids()
            .into_iter()
            .collect()
    }

    /// Whether `filter` is refused as `--where`.
    pub(super) fn refuses(&self, filter: &str) -> bool {
        read_filters(&self.conn, &self.texts(&[filter])).is_err()
    }

    /// `canon facts --key <key>` over the whole selection.
    pub(super) fn distribution(&mut self, key: &str, limit: usize) -> Distribution {
        let ids = self.all();
        let keys = self.read(Some(key), &[], false);
        let main = keys.main.expect("a main key");
        compute_distribution(&mut self.conn, &ids, &main, limit, &self.zone).unwrap()
    }

    /// `canon facts --key <key> [--by-root] [--group-by …]` over the whole selection.
    pub(super) fn grouped(
        &mut self,
        key: &str,
        grouping: &[&str],
        by_root: bool,
        limit: usize,
    ) -> Grouped {
        let ids = self.all();
        let keys = self.read(Some(key), grouping, by_root);
        let main = keys.main.expect("a main key");
        compute_grouped_distribution(
            &mut self.conn,
            &ids,
            &main,
            &keys.grouping,
            limit,
            &self.zone,
        )
        .unwrap()
    }

    /// A report's keys as `canon facts` reads them: read and bound, then each given its default.
    pub(super) fn read(
        &self,
        key: Option<&str>,
        grouping: &[&str],
        by_root: bool,
    ) -> crate::facts::ops::report::ReportKeys {
        let grouping: Vec<String> = grouping.iter().map(|text| text.to_string()).collect();
        let reading = read_report(&self.conn, key, &grouping, &self.texts(&[]))
            .unwrap_or_else(|error| panic!("{key:?} {grouping:?} does not read: {error}"));
        report_keys(reading.key, reading.grouping, by_root).unwrap()
    }
}

/// A distribution's value rows, each as `(cell, count)`, then `(no value)`'s.
pub(super) fn cells(distribution: &Distribution) -> Vec<(String, i64)> {
    distribution
        .rows
        .iter()
        .chain(&distribution.no_value)
        .map(|row| (row.cell.clone(), row.count))
        .collect()
}

/// A grouped row's cells, without a mixed value's type.
pub(super) fn cells_of(parts: &[crate::facts::ops::report::Part]) -> Vec<String> {
    parts.iter().map(|part| part.cell.clone()).collect()
}

/// A grouped row's cells joined as the row prints them, without a mixed value's type.
pub(super) fn joined(parts: &[crate::facts::ops::report::Part]) -> String {
    cells_of(parts).join(" / ")
}

/// Every line under a table, as printed.
pub(super) fn printed_lines(lines: &[crate::facts::ops::report::Line]) -> Vec<String> {
    lines
        .iter()
        .map(|line| match &line.filter {
            Some(filter) => format!("{}: {filter}", line.label),
            None => line.label.clone(),
        })
        .collect()
}

/// The text after `: ` on the line whose label is `label`.
pub(super) fn line_filter<'a>(
    lines: &'a [crate::facts::ops::report::Line],
    label: &str,
) -> &'a str {
    lines
        .iter()
        .find(|line| line.label == label)
        .and_then(|line| line.filter.as_deref())
        .unwrap_or_else(|| panic!("no line {label:?} in {:?}", printed_lines(lines)))
}

/// Each source's value for `key` as the report reads it, by the row it is counted under — the
/// row's cell, and on a key of mixed type its value's type — so a row's filter can be checked
/// against the sources that row counted.
pub(super) fn counted(
    library: &mut Library,
    key: &crate::expr::ParsedFactKey,
) -> BTreeMap<(String, Option<&'static str>), BTreeSet<i64>> {
    let ids = library.all();
    let fetched = crate::core::repo::source::batch_fetch_by_ids(&library.conn, &ids).unwrap();
    let sources: Vec<_> = ids.iter().map(|id| &fetched[id]).collect();
    let values =
        crate::expr::fetch_row_values(&mut library.conn, &sources, &[key], &library.zone).unwrap();
    let mut rows: BTreeMap<(String, Option<&'static str>), BTreeSet<i64>> = BTreeMap::new();
    for (id, read) in values {
        let row = match read.into_iter().next().flatten() {
            Some(value) => (
                key.row(&value).cell,
                key.mixed_types().map(|_| value.type_word()),
            ),
            None => ("(no value)".to_string(), None),
        };
        rows.entry(row).or_default().insert(id);
    }
    rows
}
