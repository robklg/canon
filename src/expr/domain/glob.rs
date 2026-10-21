//! **A glob**: the text after `~` or `!~`, read once into pieces, and its matcher. The reader
//! calls [`read_glob`] at that position and owns everything around it; the matcher walks the
//! pieces and never reads a character of the glob again, so quoted text stays literal.
//!
//! Read as a shell reads it: `*` any run, crossing `/`; `?` one visible character; sets
//! `[abc]`, `[a-z]`, `[!abc]`, `[^abc]`, a set a space reaches before its `]` unclosed;
//! quoted text literal, joined to the unquoted text beside it. Text compares exactly as
//! stored, code point by code point; `?` and a set member take one whole visible character (an extended grapheme cluster), and only from where one
//! starts in the stored text. The matcher holds the set of positions it can reach after each
//! piece, so its work is bounded by pieces × (length + 1) × the longest text piece.

use unicode_segmentation::UnicodeSegmentation;

use super::problem::{cut, Fix, Place, Problem};
use super::read::{is_bare_value_char, is_space, read_quoted, Cursor};

/// A glob as read: its pieces, in order.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct Glob {
    pieces: Vec<Piece>,
}

#[derive(Debug, Clone, PartialEq)]
enum Piece {
    /// Text that matches itself, code point by code point.
    Text(String),
    /// `*`: any run, none included.
    Any,
    /// `?`: one visible character.
    One,
    /// `[…]`: one visible character from a set, or not from it.
    Set(Set),
}

#[derive(Debug, Clone, PartialEq)]
struct Set {
    negated: bool,
    members: Vec<Member>,
}

#[derive(Debug, Clone, PartialEq)]
enum Member {
    /// One visible character, as typed.
    Char(String),
    /// Every code point from the first end to the second, where both are one code point.
    Range(String, String),
}

/// A glob as typed: its reading, its text and place, and, for each part holding a character an
/// unquoted glob may not hold, the byte of the first such character. Binding words them, since
/// the sentence depends on the key's type: a key of another type gets its type sentence instead.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct InputGlob {
    pub glob: Glob,
    pub input: String,
    pub place: Place,
    pub needs_quotes: Vec<usize>,
}

/// The sentence under a part of a glob holding `c`, a character an unquoted glob may not hold.
pub(in crate::expr) fn needs_quotes_sentence(c: char) -> String {
    format!("`{c}` needs quotes; quote that part, but not * ? [ ]")
}

/// **The problem under a part's first character that needs quotes**: the character `rest`
/// starts with, at byte `at` of the text `of` sits in, with no fix. One builder: binding words a glob that was read, the reader one where
/// reading stopped.
pub(in crate::expr) fn needs_quotes_problem(rest: &str, at: usize, of: Place) -> Problem {
    let c = rest.chars().next().unwrap_or_default();
    Problem {
        place: Place {
            start: at,
            end: at + c.len_utf8(),
            ..of
        },
        sentence: needs_quotes_sentence(c),
        rule_line: None,
        fix: Fix::None,
    }
}

/// **Read a glob** at the cursor, after `~` or `!~`. It ends where any value ends: at a space
/// outside quotes, or at a `)` closing no `(` opened inside it; a set a space reaches first is
/// unclosed. A quote mark anywhere opens quoted text; `[` opens a set, read to its `]`; `]`
/// outside a set is text. A part — a run
/// of unquoted text between the glob's ends, `*`, `?`, a set and quoted text — holding a
/// character a plain value may not hold records its first one. `None` where no value stands, or
/// where reading stopped at an unclosed quote or set; then the parts read before the stop are
/// worded here, since the condition is never bound.
pub(in crate::expr) fn read_glob(cursor: &mut Cursor) -> Option<InputGlob> {
    let start = cursor.at();
    match cursor.peek() {
        None | Some(')') => return None,
        Some(c) if is_space(c) => return None,
        Some(_) => {}
    }
    let mut pieces = Vec::new();
    let mut text = String::new();
    let mut needs_quotes = Vec::new();
    let mut part_noted = false;
    let mut depth = 0usize;
    while let Some(c) = cursor.peek() {
        let at = cursor.at();
        match c {
            c if is_space(c) => break,
            ')' if depth == 0 => break,
            '"' | '\'' => {
                if quote_is_unclosed(cursor) {
                    word_needs_quotes(cursor, &needs_quotes);
                }
                text.push_str(&read_quoted(cursor)?);
                part_noted = false;
            }
            '*' | '?' => {
                flush(&mut pieces, &mut text);
                pieces.push(if c == '*' { Piece::Any } else { Piece::One });
                part_noted = false;
                cursor.bump();
            }
            '[' => {
                let Some(set) = read_set(cursor) else {
                    word_needs_quotes(cursor, &needs_quotes);
                    cursor.problem(at, at + 1, "this set is unclosed", None, Fix::None);
                    cursor.stop();
                    return None;
                };
                flush(&mut pieces, &mut text);
                pieces.push(Piece::Set(set));
                part_noted = false;
            }
            c => {
                match c {
                    '(' => depth += 1,
                    ')' => depth -= 1,
                    _ => {}
                }
                if !(is_bare_value_char(c) || c == ']' || part_noted) {
                    needs_quotes.push(at);
                    part_noted = true;
                }
                text.push(c);
                cursor.bump();
            }
        }
    }
    flush(&mut pieces, &mut text);
    let end = cursor.at();
    Some(InputGlob {
        glob: Glob { pieces },
        input: cursor.slice(start, end).to_string(),
        place: cursor.place(start, end),
        needs_quotes,
    })
}

/// The pending text, as one piece.
fn flush(pieces: &mut Vec<Piece>, text: &mut String) {
    if !text.is_empty() {
        pieces.push(Piece::Text(std::mem::take(text)));
    }
}

/// Whether the quote at the cursor never closes, found on a scratch cursor so nothing is
/// recorded and the parts before it can be worded first.
fn quote_is_unclosed(cursor: &Cursor) -> bool {
    let mut scan = Cursor::new(cursor.text(), 0);
    scan.set_at(cursor.at());
    read_quoted(&mut scan).is_none()
}

/// Record a problem under each part's first character that needs quotes.
fn word_needs_quotes(cursor: &mut Cursor, needs_quotes: &[usize]) {
    for &at in needs_quotes {
        let rest = cut(cursor.text(), at, cursor.text().len());
        let problem = needs_quotes_problem(rest, at, cursor.place(at, at));
        let Place { start, end, .. } = problem.place;
        let rule_line = problem.rule_line.as_deref();
        cursor.problem(start, end, problem.sentence, rule_line, problem.fix);
    }
}

/// A set, from its `[` to its `]`: right after `[`, `!` or `^` negates; then a `]` is a member,
/// and every other character up to the next `]` is itself, a quote mark included. A space ends
/// the glob inside a set as outside one, so a set holds none. Members are visible characters;
/// `x-y` between two members is a range, a `-` first or last a member. `None`, the cursor
/// unmoved, where no `]` closes it before a space or the end.
fn read_set(cursor: &mut Cursor) -> Option<Set> {
    let open = cursor.at();
    let rest = cut(cursor.text(), open + 1, cursor.text().len());
    let negated = rest.starts_with(['!', '^']);
    let from = usize::from(negated);
    let search = if cut(rest, from, rest.len()).starts_with(']') {
        from + 1
    } else {
        from
    };
    let close = search + cut(rest, search, rest.len()).find(|c| c == ']' || is_space(c))?;
    if !cut(rest, close, rest.len()).starts_with(']') {
        return None;
    }
    let graphemes: Vec<&str> = cut(rest, from, close).graphemes(true).collect();
    let mut members = Vec::new();
    let mut i = 0;
    while i < graphemes.len() {
        if i + 2 < graphemes.len() && graphemes[i + 1] == "-" {
            members.push(Member::Range(
                graphemes[i].to_string(),
                graphemes[i + 2].to_string(),
            ));
            i += 3;
        } else {
            members.push(Member::Char(graphemes[i].to_string()));
            i += 1;
        }
    }
    cursor.set_at(open + 1 + close + 1);
    Some(Set { negated, members })
}

#[cfg(test)]
thread_local! {
    /// The matcher's steps, counted for the polynomial pin.
    static STEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn step() {
    #[cfg(test)]
    STEPS.with(|s| s.set(s.get() + 1));
}

/// The one code point a text is, where it is exactly one.
fn single(text: &str) -> Option<char> {
    let mut chars = text.chars();
    let c = chars.next()?;
    chars.next().is_none().then_some(c)
}

impl Set {
    /// Whether the set takes `run`, one visible character of the stored text: equal to a
    /// member code point for code point, or one code point between a range's ends.
    fn takes(&self, run: &[char]) -> bool {
        let member = self.members.iter().any(|m| match m {
            Member::Char(text) => text.chars().eq(run.iter().copied()),
            Member::Range(from, to) => match (single(from), single(to), run) {
                (Some(from), Some(to), [c]) => (from..=to).contains(c),
                _ => false,
            },
        });
        member != self.negated
    }
}

impl Glob {
    /// **Whether `text` matches the glob as a whole.** Text pieces and `*` move by code points;
    /// `?` and a set take exactly one visible character, starting only where one starts in
    /// `text` — so after a text piece ending inside a decomposed `é`, `?` takes nothing.
    pub(in crate::expr) fn matches(&self, text: &str) -> bool {
        let chars: Vec<char> = text.chars().collect();
        let n = chars.len();
        let by_character = self
            .pieces
            .iter()
            .any(|p| matches!(p, Piece::One | Piece::Set(_)));
        // Where each visible character that starts at a position ends; `None` inside one.
        let ends: Vec<Option<usize>> = if by_character {
            let mut ends = vec![None; n + 1];
            let mut at = 0;
            for g in text.graphemes(true) {
                let len = g.chars().count();
                ends[at] = Some(at + len);
                at += len;
            }
            ends
        } else {
            Vec::new()
        };
        let mut reach = vec![false; n + 1];
        reach[0] = true;
        for piece in &self.pieces {
            let mut next = vec![false; n + 1];
            match piece {
                Piece::Text(t) => {
                    let t: Vec<char> = t.chars().collect();
                    for p in (0..=n).filter(|&p| reach[p]) {
                        step();
                        if chars.get(p..p + t.len()) == Some(&t[..]) {
                            next[p + t.len()] = true;
                        }
                    }
                }
                Piece::Any => {
                    if let Some(first) = reach.iter().position(|r| *r) {
                        for slot in &mut next[first..] {
                            step();
                            *slot = true;
                        }
                    }
                }
                Piece::One | Piece::Set(_) => {
                    for p in (0..n).filter(|&p| reach[p]) {
                        step();
                        let Some(end) = ends[p] else { continue };
                        let takes = match piece {
                            Piece::Set(set) => set.takes(&chars[p..end]),
                            Piece::One | Piece::Text(_) | Piece::Any => true,
                        };
                        if takes {
                            next[end] = true;
                        }
                    }
                }
            }
            reach = next;
        }
        reach[n]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What reading `text` as a glob gives: the glob, and every problem as (byte, sentence).
    fn read(text: &str) -> (Option<InputGlob>, Vec<(usize, String)>, usize) {
        let mut cursor = Cursor::new(text, 0);
        let glob = read_glob(&mut cursor);
        let at = cursor.at();
        let problems = cursor
            .into_problems()
            .into_iter()
            .map(|p| (p.place.start, p.sentence))
            .collect();
        (glob, problems, at)
    }

    /// `text` read whole as a glob with nothing to quote.
    fn glob(text: &str) -> Glob {
        let (glob, problems, at) = read(text);
        assert!(problems.is_empty(), "{text}: {problems:?}");
        assert_eq!(at, text.len(), "{text}: read to {at}");
        let glob = glob.unwrap_or_else(|| panic!("{text}: nothing read"));
        assert!(
            glob.needs_quotes.is_empty(),
            "{text}: {:?}",
            glob.needs_quotes
        );
        glob.glob
    }

    /// Which of `names` the glob `text` matches.
    fn selects<'n>(text: &str, names: &[&'n str]) -> Vec<&'n str> {
        let g = glob(text);
        names.iter().copied().filter(|n| g.matches(n)).collect()
    }

    const CAFE_NFC: &str = "Caf\u{e9}.jpg";
    const CAFE_NFD: &str = "Cafe\u{301}.jpg";

    #[test]
    fn quoted_text_in_a_glob_is_literal() {
        let names = [
            "Holiday [2024].jpg",
            "Holiday 2.jpg",
            "*star.jpg",
            "star.jpg",
            "IMG_0001.jpg",
            "IMG_*",
            "IMG_0002.jpg",
        ];
        assert_eq!(selects(r#"*"[2024]"*"#, &names), ["Holiday [2024].jpg"]);
        assert_eq!(selects(r#""*"*"#, &names), ["*star.jpg"]);
        assert_eq!(selects(r#"IMG"_0001".jpg"#, &names), ["IMG_0001.jpg"]);
        assert_eq!(selects(r#""IMG_*""#, &names), ["IMG_*"]);
        assert_eq!(selects("'IMG_*'", &names), ["IMG_*"]);
        // The quoted text joins the text beside it: one piece.
        assert_eq!(
            glob(r#"IMG"_0001".jpg"#).pieces,
            [Piece::Text("IMG_0001.jpg".into())]
        );
    }

    #[test]
    fn question_mark_takes_one_visible_character() {
        assert!(glob("Caf?.jpg").matches(CAFE_NFC));
        assert!(glob("Caf?.jpg").matches(CAFE_NFD));
        assert!(!glob("Caf??.jpg").matches(CAFE_NFC));
        assert!(!glob("Caf??.jpg").matches(CAFE_NFD));
        assert!(glob("??.jpg").matches("ab.jpg"));
        assert!(!glob("??.jpg").matches("abc.jpg"));
        assert!(glob("photo.???").matches("photo.jpg"));
        assert!(!glob("photo.???").matches("photo.jpeg"));
        // An emoji with a skin tone, a family joined by zero-width joiners, a heart with its
        // emoji selector: each one visible character.
        assert!(glob("?.jpg").matches("\u{1F44D}\u{1F3FD}.jpg"));
        assert!(glob("?.jpg").matches("\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}.jpg"));
        assert!(glob("I?NY.jpg").matches("I\u{2764}\u{FE0F}NY.jpg"));
        assert!(!glob("??.jpg").matches("\u{1F44D}\u{1F3FD}.jpg"));
        // A Hangul syllable written as three jamo, and a Devanagari consonant with its vowel
        // sign: one visible character each.
        assert!(glob("?.jpg").matches("\u{1112}\u{1161}\u{11AB}.jpg"));
        assert!(glob("??.jpg").matches("\u{0930}\u{093E}\u{092E}.jpg"));
        assert!(!glob("?").matches(""));
        assert!(!glob("?").matches("ab"));
    }

    #[test]
    fn a_question_mark_starts_only_at_a_visible_character() {
        // `Cafe` ends inside the decomposed `é`; `?` cannot take the rest of it.
        assert!(!glob("Cafe?.jpg").matches(CAFE_NFD));
        assert!(!glob("Cafe?.jpg").matches(CAFE_NFC));
        // Text compares exactly as stored: `Cafe*` reaches into the decomposed form only.
        assert!(glob("Cafe*").matches(CAFE_NFD));
        assert!(!glob("Cafe*").matches(CAFE_NFC));
    }

    #[test]
    fn a_set_member_is_one_visible_character() {
        let precomposed = glob("Caf[\u{e9}].jpg");
        assert!(precomposed.matches(CAFE_NFC));
        assert!(!precomposed.matches(CAFE_NFD));
        let decomposed = glob("Caf[e\u{301}].jpg");
        assert!(decomposed.matches(CAFE_NFD));
        assert!(!decomposed.matches(CAFE_NFC));
        // A decomposed `é` is one character, not the member `e`.
        assert!(!glob("Caf[e].jpg").matches(CAFE_NFD));
        assert!(glob("[abc]").matches("a"));
        assert!(!glob("[abc]").matches("d"));
        assert!(glob("[a-z]").matches("m"));
        assert!(!glob("[a-z]").matches("M"));
        assert!(glob("[a-zA-Z]*").matches("Zb.jpg"));
        assert!(!glob("[a-zA-Z]*").matches("1b.jpg"));
    }

    #[test]
    fn sets_negate_with_bang_and_caret() {
        for text in ["[!abc]*", "[^abc]*"] {
            assert!(glob(text).matches("d.jpg"), "{text}");
            assert!(!glob(text).matches("a.jpg"), "{text}");
            assert!(!glob(text).matches(""), "{text}");
        }
        // A negated set takes one whole visible character that is no member.
        assert!(glob("Caf[!e].jpg").matches(CAFE_NFD));
        assert!(!glob("Caf[!\u{e9}].jpg").matches(CAFE_NFC));
    }

    #[test]
    fn inside_a_set_a_quote_is_a_member_and_a_space_ends_it() {
        assert_eq!(selects("[@#]*", &["@a", "#b", "c"]), ["@a", "#b"]);
        // A space ends the glob inside a set too: the set is unclosed, whatever follows, a
        // negated set, a `]` first and a quote mark before the space included.
        for text in [
            "*[ _]*",
            "[abc OR filename~x]",
            "[] ]",
            "[! ]",
            "[!] ]",
            r#"*[" "]*"#,
            "[a\tb]",
        ] {
            let (glob, problems, _) = read(text);
            assert!(glob.is_none(), "{text}");
            let at = text.find('[').unwrap();
            assert_eq!(
                problems,
                [(at, "this set is unclosed".to_string())],
                "{text}"
            );
        }
        assert_eq!(
            selects(r#"*["']*"#, &["O'Brien", "say \"hi\"", "plain"]),
            ["O'Brien", "say \"hi\""]
        );
    }

    #[test]
    fn a_bracket_first_in_a_set_is_a_member() {
        assert!(glob("[]abc]").matches("]"));
        assert!(glob("[]abc]").matches("b"));
        assert!(glob("[!]a]").matches("x"));
        assert!(!glob("[!]a]").matches("]"));
        assert!(!glob("[!]a]").matches("a"));
        for text in ["[]", "[!]", "[^]"] {
            let (glob, problems, _) = read(text);
            assert!(glob.is_none(), "{text}");
            assert_eq!(
                problems,
                [(0, "this set is unclosed".to_string())],
                "{text}"
            );
        }
    }

    #[test]
    fn a_dash_first_or_last_in_a_set_is_a_member() {
        assert_eq!(selects("[-a]", &["-", "a", "b"]), ["-", "a"]);
        assert_eq!(selects("[a-]", &["-", "a", "b"]), ["-", "a"]);
        assert_eq!(selects("[-_]*", &["-x", "_x", "x"]), ["-x", "_x"]);
    }

    #[test]
    fn a_range_matches_nothing_when_reversed_or_an_end_is_more_than_one_code_point() {
        assert!(selects("[z-a]", &["a", "m", "z"]).is_empty());
        // A range whose end is a decomposed letter takes nothing, its other end included.
        assert!(selects("[a-e\u{301}]", &["a", "c", "e\u{301}", "\u{e9}"]).is_empty());
        // A decomposed `é` is in no range; a precomposed one only by its code point.
        assert!(!glob("[a-z]").matches("e\u{301}"));
        assert!(glob("[\u{e0}-\u{ff}]").matches("\u{e9}"));
        assert!(!glob("[\u{e0}-\u{ff}]").matches("e\u{301}"));
    }

    #[test]
    fn a_closing_bracket_outside_a_set_is_literal() {
        assert_eq!(selects("a]*", &["a]b", "ab"]), ["a]b"]);
        assert_eq!(glob("a]").pieces, [Piece::Text("a]".into())]);
    }

    #[test]
    fn a_glob_with_no_wildcard_matches_the_whole_value() {
        assert!(glob("a.jpg").matches("a.jpg"));
        assert!(!glob("a.jpg").matches("a.jpgx"));
        assert!(!glob("a.jpg").matches("xa.jpg"));
        assert!(glob(r#""""#).matches(""));
        assert!(!glob(r#""""#).matches("a"));
    }

    #[test]
    fn star_crosses_slash() {
        assert!(glob("*/2024/*").matches("a/b/2024/c.jpg"));
        assert!(glob("image/*").matches("image/x/y"));
        assert!(glob("*").matches(""));
        assert!(glob("*").matches("anything"));
        assert!(glob("*.jpg").matches("photo.jpg"));
        assert!(!glob("*.jpg").matches("photo.png"));
    }

    #[test]
    fn a_backslash_needs_quotes_and_is_literal_inside_them() {
        // A backslash escapes nothing: unquoted it needs quotes, quoted it is itself.
        let (read_glob, problems, _) = read(r"\*");
        assert!(problems.is_empty());
        assert_eq!(read_glob.unwrap().needs_quotes, [0]);
        let (read_glob, _, _) = read(r"a\*b");
        assert_eq!(read_glob.unwrap().needs_quotes, [1]);
        let names = [r"\star.jpg", "*star.jpg", "star.jpg"];
        assert_eq!(selects(r#""\"*"#, &names), [r"\star.jpg"]);
        assert_eq!(
            selects(r#""\*star.jpg""#, &[r"\*star.jpg", "*star.jpg"]),
            [r"\*star.jpg"]
        );
    }

    #[test]
    fn two_apostrophes_close_each_other() {
        let text = "*John's* OR source.rel_path~*Mary's*";
        let (read_glob, problems, at) = read(text);
        assert!(problems.is_empty());
        assert_eq!(at, text.len(), "one glob, as a shell reads it");
        let g = read_glob.unwrap();
        assert!(g.needs_quotes.is_empty());
        assert!(g.glob.matches("xJohns* OR source.rel_path~*Marysx"));
        let (read_glob, problems, _) = read("O'Brien*");
        assert!(read_glob.is_none());
        assert_eq!(problems, [(1, "this quote is unclosed".to_string())]);
    }

    #[test]
    fn two_touching_quoted_parts_in_one_mark_hold_a_quote_mark() {
        assert_eq!(glob(r#""a""b""#).pieces, [Piece::Text("a\"b".into())]);
        assert_eq!(glob(r#""a"'b'"#).pieces, [Piece::Text("ab".into())]);
    }

    /// The matcher's work is bounded by pieces × (length + 1): a glob that is exponential for
    /// a backtracking matcher takes a number of steps within that bound.
    #[test]
    fn the_matcher_stays_polynomial() {
        let g = glob("*a*a*a*a*a*a*b");
        assert_eq!(g.pieces.len(), 14);
        let name = "a".repeat(10_000);
        STEPS.with(|s| s.set(0));
        assert!(!g.matches(&name));
        let steps = STEPS.with(|s| s.get());
        assert!(steps <= 14 * 10_001, "{steps} steps");
    }
}
