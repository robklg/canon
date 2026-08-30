//! Archive's command surface: `canon cluster generate/refresh/status` and
//! `canon apply`. One declares an intended archive operation, the other
//! performs it, and they share almost nothing — so they stay separate files
//! rather than one module. What little they do share is here, where both can
//! reach it: text a user reads from either command about the same situation,
//! which must not be spelled twice.

pub(super) mod apply;
pub(super) mod cluster;

use std::path::Path;

/// The way back from a recorded scope that no longer resolves.
///
/// Both commands that meet one say this: `apply` inside the refusal that stops
/// it, `cluster status` as its next step. One sentence, because it is one
/// remedy — and the order in it is load-bearing. A refresh alone cannot heal a
/// prefix that names no known root: there is nothing to heal it to, so the
/// edit comes first and the refresh carries it into the lock.
///
/// Private, not `pub(super)`: a module's private items are already visible to
/// its descendants, so the two `cli/` files reach this and nothing in `ops`,
/// `domain` or `repo` can. An interface-layer formatter has no business being
/// reachable from a lower stratum, and the architecture test does not police
/// this file.
///
/// Only spaces are quoted, matching the suggestion `cluster generate` prints:
/// a path carrying a quote or a shell metacharacter yields a line that will
/// not run as printed.
fn edit_then_refresh(manifest: &Path) -> String {
    let path = manifest.display().to_string();
    let quoted = if path.contains(' ') {
        format!("'{path}'")
    } else {
        path
    };
    format!("Edit meta.scope, then `canon cluster refresh {quoted}` to rewrite the lock.")
}
