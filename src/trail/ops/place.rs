//! What Canon knows about a place, before the trail renders anything for it.
//!
//! A history question is deliberately more lenient than a present-tense one:
//! a folder a move-mode apply emptied has no sources standing in it, yet it
//! still has a story. That leniency needs a floor, or every mistyped path
//! borrows the history of whatever it happens to sit under.

use anyhow::Result;

use crate::core::domain::root::{find_containing_root, Root};
use crate::core::repo::Connection;
use crate::retire::RetiredScope;

/// What a place turned out to be when asked about its history.
pub(crate) enum PlaceKnowledge {
    /// A retired root covers it — the book is the answer.
    Retired(Box<RetiredScope>),
    /// Something records this place; render its history.
    Evidence,
    /// Nothing records it at all.
    Unknown,
}

/// Whether anything Canon holds records this place.
///
/// Four independent kinds of evidence, any one of which is enough:
/// sources ever known here (present or not), a note left here, an `apply`
/// that drew from or placed into here, or a decision scoped here or inside
/// it. The last is descendant-or-equal by the two-claims placement law — a
/// decision scoped at an ancestor is a claim about the ancestor, not about
/// every path anyone might type beneath it.
pub(crate) fn history_evidence_at(conn: &Connection, prefix: &str, roots: &[Root]) -> Result<bool> {
    let Some((root_id, _root_path, _role, rel_prefix)) = find_containing_root(prefix, roots) else {
        return Ok(false);
    };

    if crate::core::repo::source::sources_exist_at_scope(conn, root_id, &rel_prefix)? {
        return Ok(true);
    }
    if crate::notes::count_subtree_notes(conn, root_id, &rel_prefix)? > 0 {
        return Ok(true);
    }
    if crate::trail::repo::extraction_endpoint_at(conn, root_id, &rel_prefix, prefix)? {
        return Ok(true);
    }
    crate::trail::repo::decision_scoped_at_or_under(conn, root_id, &rel_prefix)
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
    if history_evidence_at(conn, prefix, roots)? {
        return Ok(PlaceKnowledge::Evidence);
    }
    Ok(PlaceKnowledge::Unknown)
}
