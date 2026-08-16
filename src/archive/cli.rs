//! Archive's command surface: `canon cluster generate/refresh/status` and
//! `canon apply`. The two commands share nothing with each other — one
//! declares an intended archive operation, the other performs it — so they
//! stay separate files rather than one module.

pub(super) mod apply;
pub(super) mod cluster;
