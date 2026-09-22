//! What Canon knows about a place, before the trail renders anything for it.
//!
//! A history question is deliberately more lenient than a present-tense one:
//! a folder a move-mode apply emptied has no sources standing in it, yet it
//! still has a story. That leniency needs a floor, or every mistyped path
//! borrows the history of whatever it happens to sit under.

use anyhow::Result;

use crate::core::domain::path::{join_under_root, resolve_containing_root};
use crate::core::domain::root::Root;
use crate::core::ops::scope::known_form_of_rel;
use crate::core::repo::Connection;
use crate::retire::RetiredScope;

/// What a place turned out to be when asked about its history.
pub(crate) enum PlaceKnowledge {
    /// A retired root covers it — the book is the answer.
    Retired(Box<RetiredScope>),
    /// Something records this place; render its history — at *this* path,
    /// which is the asked path healed to the byte-form Canon's own records
    /// are written in. Rendering at the typed form instead would answer
    /// "there is history here" and then show none of it.
    Evidence(String),
    /// Nothing records it at all.
    Unknown,
}

/// Whether anything Canon holds records this place, and in which byte-form.
///
/// Four independent kinds of evidence, any one of which is enough:
/// sources ever known here (present or not), a note left here, an `apply`
/// that drew from or placed into here, or a decision scoped here or inside
/// it. The last is descendant-or-equal by the two-claims placement law — a
/// decision scoped at an ancestor is a claim about the ancestor, not about
/// every path anyone might type beneath it.
///
/// **The form-tolerance law, with its four questions asked as one.** The root
/// half runs first, through the law's owner; the remainder half then runs
/// through `known_form_of_rel` with all four evidence classes OR'd into its
/// predicate. Asking the remainder half's *sources* reading here would be a
/// no-op in exactly the case this function exists for — a place with no
/// sources standing in it — so the question is the one this gate actually
/// asks, and the loop it is asked over is still the law's single spelling.
///
/// Returns the byte-form under which the evidence was found; `None` means no
/// form of this place is recorded anywhere.
pub(crate) fn history_evidence_at(
    conn: &Connection,
    prefix: &str,
    roots: &[Root],
) -> Result<Option<String>> {
    let Some(rooted) = resolve_containing_root(prefix, roots) else {
        return Ok(None);
    };
    let root_id = rooted.root_id;
    let found = known_form_of_rel(&rooted.rel_path, |rel| {
        if crate::core::repo::source::sources_exist_at_scope(conn, root_id, rel)? {
            return Ok(true);
        }
        if crate::notes::count_subtree_notes(conn, root_id, rel)? > 0 {
            return Ok(true);
        }
        // The endpoint read wants the absolute path too, for the destination
        // side's write-time snapshot — rebuilt on the matched root, so the
        // root and the remainder may each carry whichever form their disk
        // gave.
        let absolute = join_under_root(&rooted.root_path, rel);
        if crate::trail::repo::extraction_endpoint_at(conn, root_id, rel, &absolute)? {
            return Ok(true);
        }
        crate::trail::repo::decision_scoped_at_or_under(conn, root_id, rel)
    })?;
    Ok(found.map(|rel| join_under_root(&rooted.root_path, &rel)))
}

/// The chain a path with no present sources runs before the trail will
/// render anything for it: a retirement covering it answers first, then any
/// evidence at all, and otherwise it is a place Canon has never known.
pub(crate) fn place_knowledge(
    conn: &Connection,
    prefix: &str,
    roots: &[Root],
) -> Result<PlaceKnowledge> {
    if let Some(statement) = crate::retire::find_retirement_covering_path(conn, prefix)? {
        return Ok(PlaceKnowledge::Retired(Box::new(statement)));
    }
    match history_evidence_at(conn, prefix, roots)? {
        Some(found) => Ok(PlaceKnowledge::Evidence(found)),
        None => Ok(PlaceKnowledge::Unknown),
    }
}
