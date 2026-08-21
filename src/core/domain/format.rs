/// Format a number with thousands separators (e.g., 3847 → "3,847").
pub fn format_count(n: impl std::fmt::Display) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Format a byte count as a human-readable size (decimal units, one decimal
/// place above bytes: 4_080_218_931 → "4.1 GB").
///
/// **Canon states sizes in decimal units, everywhere.** KB/MB/GB/TB are the
/// decimal labels (the binary ones are KiB/MiB/GiB), and it is how storage
/// capacity is sold and reported. Every surface that prints a size calls this;
/// a second local size formatter makes two Canon outputs disagree about one
/// file under identical labels, which is exactly what `ls -l` did.
pub fn format_size(bytes: i64) -> String {
    const UNITS: [&str; 4] = ["KB", "MB", "GB", "TB"];
    if bytes < 1000 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    value /= 1000.0;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// Cap a path to `max` chars, keeping the tail (the significant end):
/// "/very/long/path/to/italy" → "...path/to/italy". Char-boundary safe.
/// Shared by every place that fits a path into a column (coverage, facts,
/// trail).
pub fn cap_path(path: &str, max: usize) -> String {
    let count = path.chars().count();
    if count <= max {
        return path.to_string();
    }
    let tail: String = path.chars().skip(count - max + 3).collect();
    format!("...{tail}")
}

/// The first `n` characters of a string, for display.
///
/// Use this instead of slicing by byte index. `&s[..n]` panics when `n` lands
/// inside a multi-byte character, and bounding `n` by `s.len()` does not help —
/// that bounds the length, not the boundary. Filenames, paths and any text read
/// back from a file the user can edit all reach display code, so the panic is
/// reachable wherever a raw byte slice is used to fit text into a column.
pub fn first_chars(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// A timestamp as a local calendar date (`2026-08-04`).
pub fn format_date(ts: i64) -> String {
    use chrono::{Local, TimeZone};
    match Local.timestamp_opt(ts, 0) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d").to_string(),
        _ => format!("@{ts}"),
    }
}

/// A coarse "how long ago" beside a date — seconds up to days.
pub fn format_time_ago(timestamp: Option<i64>, now: i64) -> String {
    match timestamp {
        None => "never".to_string(),
        Some(ts) => {
            let secs = now - ts;
            if secs < 0 {
                "just now".to_string()
            } else if secs < 60 {
                format!("{secs}s ago")
            } else if secs < 3600 {
                format!("{}m ago", secs / 60)
            } else if secs < 86400 {
                format!("{}h ago", secs / 3600)
            } else {
                format!("{}d ago", secs / 86400)
            }
        }
    }
}

/// Quote an argument for display when it wouldn't survive a shell verbatim.
/// Shared by every handoff builder (the sweep's, the story's) so the
/// round-trip law tests parse exactly what the user sees.
pub fn shell_quote(arg: &str) -> String {
    let safe = |c: char| c.is_ascii_alphanumeric() || "/.-_:@%+=,".contains(c);
    if !arg.is_empty() && arg.chars().all(safe) {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', "'\\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_count_small() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(1), "1");
        assert_eq!(format_count(99), "99");
        assert_eq!(format_count(999), "999");
    }

    #[test]
    fn format_count_with_commas() {
        assert_eq!(format_count(1000), "1,000");
        assert_eq!(format_count(10_000), "10,000");
        assert_eq!(format_count(100_000), "100,000");
        assert_eq!(format_count(1_000_000), "1,000,000");
        assert_eq!(format_count(1_234_567), "1,234,567");
    }

    #[test]
    fn format_count_negative() {
        assert_eq!(format_count(-1_i64), "-1");
        assert_eq!(format_count(-1_000_i64), "-1,000");
    }

    #[test]
    fn format_count_i64() {
        assert_eq!(format_count(42_i64), "42");
        assert_eq!(format_count(1_000_i64), "1,000");
    }

    #[test]
    fn format_size_bytes() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(1), "1 B");
        assert_eq!(format_size(999), "999 B");
    }

    #[test]
    fn format_size_unit_boundaries() {
        assert_eq!(format_size(1_000), "1.0 KB");
        assert_eq!(format_size(999_949), "999.9 KB");
        assert_eq!(format_size(1_000_000), "1.0 MB");
        assert_eq!(format_size(3_900_000_000), "3.9 GB");
        assert_eq!(format_size(35_000_000_000), "35.0 GB");
    }

    #[test]
    fn format_size_large() {
        assert_eq!(format_size(2_500_000_000_000), "2.5 TB");
        // TB is the ceiling — no overflow past the last unit.
        assert_eq!(format_size(9_000_000_000_000_000), "9000.0 TB");
    }

    #[test]
    fn cap_path_keeps_tail() {
        assert_eq!(cap_path("short", 35), "short");
        let long = "/Volumes/backup-2019/photos/vacations/italy/2016";
        let capped = cap_path(long, 20);
        assert_eq!(capped.chars().count(), 20);
        assert!(capped.starts_with("..."));
        assert!(capped.ends_with("italy/2016"));
    }

    #[test]
    fn cap_path_char_boundary_safe() {
        // Non-ASCII paths must not panic (the old per-site byte slicing did).
        let unicode = "/Vólùmes/фото/2016/Италия/поездка";
        let capped = cap_path(unicode, 12);
        assert_eq!(capped.chars().count(), 12);
        assert!(capped.starts_with("..."));
    }

    #[test]
    fn first_chars_counts_characters_not_bytes() {
        // Byte 16 of this string falls inside a three-byte character, so a
        // byte slice would panic where this returns sixteen characters.
        let multibyte = "日本語日本語日本語日本語日本語日本語";
        assert_eq!(first_chars(multibyte, 16).chars().count(), 16);
        assert_eq!(first_chars("abcdef", 3), "abc");
    }

    #[test]
    fn first_chars_returns_the_whole_string_when_shorter() {
        assert_eq!(first_chars("abc", 16), "abc");
        assert_eq!(first_chars("", 16), "");
    }
}
