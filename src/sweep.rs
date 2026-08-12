//! The sweep subsystem: the finder seat — the universe-wide
//! reduction-opportunities leaderboard.
//!
//! `domain` holds the structural computation (five pipeline stages) and the
//! reduction lens derived from it; `ops` composes the fetch, inclusion
//! policy, and lens ranking into the report; `cli` is the interface layer
//! (`canon sweep`) — parse args, call ops, format output.

mod cli;
mod domain;
mod ops;

pub use cli::run;
