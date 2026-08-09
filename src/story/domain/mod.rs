//! Story's domain layer: pure logic for the story review — the judgment
//! instrument that renders a root's resolution story as a map of places
//! (`canon roots story`). No I/O anywhere in this layer.

pub mod acts;
pub mod locations;
pub mod place;
pub mod splitter;

#[cfg(test)]
mod tests;
