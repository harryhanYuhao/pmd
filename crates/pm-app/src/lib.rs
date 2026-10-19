//! Process plumbing shared by the binaries (`pmd`, `pmt`): logging setup, shutdown signals and
//! the ordered stop of long-running tasks.

pub mod logging;
mod shutdown;

pub use logging::Console;
pub use shutdown::{shutdown_signal, stop_tasks};
