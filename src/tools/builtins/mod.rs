//! Built-in tool implementations for Bedrock.
//!
//! These are the core tools available out of the box:
//! - `read_file` — Read file contents
//! - `write_file` — Create or overwrite a file
//! - `edit_file` — Search-and-replace within a file
//! - `shell_exec` — Execute a shell command

mod read_file;
mod write_file;
mod edit_file;
mod shell_exec;
mod submit_task;

pub use read_file::ReadFileTool;
pub use write_file::WriteFileTool;
pub use edit_file::EditFileTool;
pub use shell_exec::ShellExecTool;
pub use submit_task::SubmitTaskTool;
use crate::tools::mcp::BridgeMcp;

use super::registry::ToolRegistry;

/// Create a ToolRegistry with all built-in tools registered.
pub fn create_default_registry() -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(ReadFileTool));
    registry.register(Box::new(WriteFileTool));
    registry.register(Box::new(EditFileTool));
    registry.register(Box::new(ShellExecTool));
    registry.register(Box::new(SubmitTaskTool));
    registry.register(Box::new(BridgeMcp));
    registry
}
