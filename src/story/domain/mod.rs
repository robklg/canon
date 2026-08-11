//! Story's domain layer: pure logic for the story review — the judgment
//! instrument that renders a root's resolution story as a map of places
//! (`canon roots story`). No I/O anywhere in this layer.

pub(super) mod acts;
pub(super) mod locations;
pub(super) mod place;
pub(super) mod splitter;

#[cfg(test)]
mod tests;
