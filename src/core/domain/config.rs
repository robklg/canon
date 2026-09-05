/// Ledger configuration loaded from `$CANON_HOME/config.toml`.
///
/// All parsing is done with per-field validation: invalid values fall back to
/// defaults with warnings. Config errors never prevent Canon from operating.

#[derive(Debug, Clone, PartialEq)]
pub enum RecordingMode {
    Full,
    Records,
    Off,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReceiptLayout {
    Central,
    Alongside,
}

#[derive(Debug, Clone)]
pub struct LedgerConfig {
    pub recording: RecordingMode,
    pub layout: ReceiptLayout,
    pub root: Option<i64>,
}

impl Default for LedgerConfig {
    fn default() -> Self {
        Self {
            recording: RecordingMode::Full,
            layout: ReceiptLayout::Central,
            root: None,
        }
    }
}

/// Parse a config file's content into `LedgerConfig`.
///
/// Returns defaults for any invalid or missing fields, with a warning message
/// per invalid field. Unknown keys and sections are silently ignored.
///
/// Mode and layout names are read case-insensitively, against one documented
/// spelling each (`"Full"`, `"Central"`). One spelling because two would be
/// two things to keep true; case-insensitive because the fallback is not
/// neutral — an unread value falls through to the default, and the default
/// *records*, so a `recording = "Off"` Canon failed to recognise would record
/// everything the user asked it not to.
pub fn parse_ledger_config(content: &str) -> (LedgerConfig, Vec<String>) {
    let mut config = LedgerConfig::default();
    let mut warnings = Vec::new();

    let value: toml::Value = match toml::from_str(content) {
        Ok(v) => v,
        Err(e) => {
            warnings.push(format!(
                "Warning: config.toml parse error ({e}), using defaults"
            ));
            return (config, warnings);
        }
    };

    let Some(ledger) = value.get("ledger") else {
        return (config, warnings);
    };

    if let Some(recording) = ledger.get("recording") {
        match recording.as_str() {
            Some(s) if s.eq_ignore_ascii_case("full") => config.recording = RecordingMode::Full,
            Some(s) if s.eq_ignore_ascii_case("records") => {
                config.recording = RecordingMode::Records
            }
            Some(s) if s.eq_ignore_ascii_case("off") => config.recording = RecordingMode::Off,
            Some(other) => warnings.push(format!(
                "Warning: unknown recording mode {:?} in config.toml, using \"Full\"",
                other
            )),
            None => warnings.push(
                "Warning: invalid recording value in config.toml (expected string), using \"Full\""
                    .to_string(),
            ),
        }
    }

    if let Some(layout) = ledger.get("layout") {
        match layout.as_str() {
            Some(s) if s.eq_ignore_ascii_case("central") => config.layout = ReceiptLayout::Central,
            Some(s) if s.eq_ignore_ascii_case("alongside") => {
                config.layout = ReceiptLayout::Alongside
            }
            Some(other) => warnings.push(format!(
                "Warning: unknown layout {:?} in config.toml, using \"Central\"",
                other
            )),
            None => warnings.push(
                "Warning: invalid layout value in config.toml (expected string), using \"Central\""
                    .to_string(),
            ),
        }
    }

    if let Some(root) = ledger.get("root") {
        match root.as_integer() {
            Some(id) => config.root = Some(id),
            None => warnings.push(
                "Warning: invalid root value in config.toml (expected integer), ignoring"
                    .to_string(),
            ),
        }
    }

    (config, warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = LedgerConfig::default();
        assert_eq!(config.recording, RecordingMode::Full);
        assert_eq!(config.layout, ReceiptLayout::Central);
        assert_eq!(config.root, None);
    }

    #[test]
    fn test_parse_valid_full() {
        let content = r#"
[ledger]
recording = "records"
layout = "alongside"
root = 5
"#;
        let (config, warnings) = parse_ledger_config(content);
        assert_eq!(config.recording, RecordingMode::Records);
        assert_eq!(config.layout, ReceiptLayout::Alongside);
        assert_eq!(config.root, Some(5));
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_parse_recording_full() {
        let (config, warnings) = parse_ledger_config("[ledger]\nrecording = \"full\"");
        assert_eq!(config.recording, RecordingMode::Full);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_parse_recording_records() {
        let (config, warnings) = parse_ledger_config("[ledger]\nrecording = \"records\"");
        assert_eq!(config.recording, RecordingMode::Records);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_parse_recording_off() {
        let (config, warnings) = parse_ledger_config("[ledger]\nrecording = \"off\"");
        assert_eq!(config.recording, RecordingMode::Off);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_parse_recording_invalid() {
        let (config, warnings) = parse_ledger_config("[ledger]\nrecording = \"nope\"");
        assert_eq!(config.recording, RecordingMode::Full);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("nope"));
    }

    #[test]
    fn test_parse_layout_central() {
        let (config, warnings) = parse_ledger_config("[ledger]\nlayout = \"central\"");
        assert_eq!(config.layout, ReceiptLayout::Central);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_parse_layout_alongside() {
        let (config, warnings) = parse_ledger_config("[ledger]\nlayout = \"alongside\"");
        assert_eq!(config.layout, ReceiptLayout::Alongside);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_parse_layout_invalid() {
        let (config, warnings) = parse_ledger_config("[ledger]\nlayout = \"flat\"");
        assert_eq!(config.layout, ReceiptLayout::Central);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("flat"));
    }

    #[test]
    fn test_parse_root_present() {
        let (config, warnings) = parse_ledger_config("[ledger]\nroot = 42");
        assert_eq!(config.root, Some(42));
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_parse_root_absent() {
        let (config, warnings) = parse_ledger_config("[ledger]");
        assert_eq!(config.root, None);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_parse_empty_content() {
        let (config, warnings) = parse_ledger_config("");
        assert_eq!(config.recording, RecordingMode::Full);
        assert_eq!(config.layout, ReceiptLayout::Central);
        assert_eq!(config.root, None);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_parse_unknown_keys_ignored() {
        let content = r#"
[ledger]
recording = "full"
unknown_key = "whatever"
"#;
        let (config, warnings) = parse_ledger_config(content);
        assert_eq!(config.recording, RecordingMode::Full);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_parse_unknown_sections_ignored() {
        let content = r#"
[ledger]
recording = "full"

[foo]
bar = "baz"
"#;
        let (config, warnings) = parse_ledger_config(content);
        assert_eq!(config.recording, RecordingMode::Full);
        assert!(warnings.is_empty());
    }

    // ========================================================================
    // Reading a name whatever its case
    // ========================================================================

    /// Every documented recording spelling lands on the mode it names. The
    /// register writes them capitalised; the parser read only lowercase, so a
    /// line copied from the documentation fell through to the default — and
    /// the default records, which is the opposite of what `"Off"` asked for.
    #[test]
    fn a_recording_mode_is_read_by_name_whatever_its_case() {
        for (written, expected) in [
            ("Full", RecordingMode::Full),
            ("FULL", RecordingMode::Full),
            ("full", RecordingMode::Full),
            ("Records", RecordingMode::Records),
            ("RECORDS", RecordingMode::Records),
            ("records", RecordingMode::Records),
            ("Off", RecordingMode::Off),
            ("OFF", RecordingMode::Off),
            ("off", RecordingMode::Off),
        ] {
            let (config, warnings) =
                parse_ledger_config(&format!("[ledger]\nrecording = \"{written}\""));
            assert_eq!(config.recording, expected, "recording = {written:?}");
            assert!(warnings.is_empty(), "recording = {written:?}: {warnings:?}");
        }
    }

    /// The sibling field, the same defect: `layout` is documented capitalised
    /// too, and a layout misread lands receipts somewhere other than where the
    /// config asked for them.
    #[test]
    fn a_layout_is_read_by_name_whatever_its_case() {
        for (written, expected) in [
            ("Central", ReceiptLayout::Central),
            ("CENTRAL", ReceiptLayout::Central),
            ("central", ReceiptLayout::Central),
            ("Alongside", ReceiptLayout::Alongside),
            ("ALONGSIDE", ReceiptLayout::Alongside),
            ("alongside", ReceiptLayout::Alongside),
        ] {
            let (config, warnings) =
                parse_ledger_config(&format!("[ledger]\nlayout = \"{written}\""));
            assert_eq!(config.layout, expected, "layout = {written:?}");
            assert!(warnings.is_empty(), "layout = {written:?}: {warnings:?}");
        }
    }

    /// A warning names the fallback in the spelling the register documents —
    /// a message is a surface, and one teaching a spelling the docs do not
    /// carry is how a second spelling gets into circulation.
    #[test]
    fn a_fallback_is_named_in_the_documented_spelling() {
        let (_, warnings) =
            parse_ledger_config("[ledger]\nrecording = \"nope\"\nlayout = \"flat\"");
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(warnings[0].contains("\"Full\""), "{warnings:?}");
        assert!(warnings[1].contains("\"Central\""), "{warnings:?}");
    }
}
