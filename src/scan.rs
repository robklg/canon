mod cli;
mod domain;
mod ops;
mod repo;

pub use cli::{find_candidates, run};
// The physical-identity law and the two types naming its subject. Production
// consumes the law from inside this subsystem; the contentless-law canary is
// the only consumer of this re-export, reaching it through the barrel like the
// other subsystems' test-only riders. It asks the law directly because the
// relocation refusal is where an empty file's vacuous evidence would otherwise
// move a row, and no other surface states that.
#[allow(unused_imports)]
pub use domain::{same_physical_file, FileObservation, IdentityClaim};
