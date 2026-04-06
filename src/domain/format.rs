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
}
