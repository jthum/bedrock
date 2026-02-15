use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::path::PathBuf;

use crate::tools::{parse_args, Tool, ToolContext, ToolError, ToolOutput};

pub struct ReadFileTool;

#[derive(Deserialize)]
struct ReadFileArgs {
    /// Path to read (relative to workspace root, or absolute)
    path: String,
}

#[async_trait]
impl Tool for ReadFileTool {
    fn name(&self) -> &str {
        "read_file"
    }

    fn description(&self) -> &str {
        "Read the contents of a file. Returns the full file content as text."
    }

    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the file to read (relative to workspace root, or absolute)"
                }
            },
            "required": ["path"]
        })
    }

    #[tracing::instrument(skip(self, params, ctx), fields(path = %params["path"].as_str().unwrap_or("unknown")))]
    async fn execute(&self, params: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: ReadFileArgs = parse_args(params)?;
        tracing::info!(path = %args.path, "Reading file");
        let path = resolve_path(&args.path, &ctx.workspace_root);

        // Security: validate path is within workspace
        validate_workspace_path(&path, &ctx.workspace_root)?;

        let content = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| ToolError::ExecutionError(format!("Failed to read {}: {}", path.display(), e)))?;

        Ok(ToolOutput {
            content,
            metadata: serde_json::json!({
                "path": path.display().to_string(),
                "bytes": path.metadata().map(|m| m.len()).unwrap_or(0),
            }),
        })
    }
}

/// Resolve a path relative to the workspace root.
pub(crate) fn resolve_path(path: &str, workspace_root: &std::path::Path) -> PathBuf {
    let p = PathBuf::from(path);
    if p.is_absolute() {
        p
    } else {
        workspace_root.join(p)
    }
}

/// Validate that a resolved path is within the workspace root.
pub(crate) fn validate_workspace_path(
    path: &std::path::Path,
    workspace_root: &std::path::Path,
) -> Result<(), ToolError> {
    // Canonicalize the workspace root once
    let canonical_root = workspace_root.canonicalize().map_err(|e| {
        ToolError::ExecutionError(format!(
            "Failed to canonicalize workspace root '{}': {}",
            workspace_root.display(),
            e
        ))
    })?;

    // Resolving the path to check.
    // If the path exists, canonicalize it.
    // If it doesn't exist, we need to check its parent.
    let canonical_path = if path.exists() {
        path.canonicalize().map_err(|e| {
            ToolError::ExecutionError(format!("Failed to canonicalize path '{}': {}", path.display(), e))
        })?
    } else {
        // For non-existent files (e.g. write_file to new file), check the parent dir
        let parent = path.parent().ok_or_else(|| {
            ToolError::PermissionDenied(format!(
                "Cannot write to root path '{}'",
                path.display()
            ))
        })?;

        let canonical_parent = parent.canonicalize().map_err(|e| {
            ToolError::ExecutionError(format!("Failed to canonicalize parent directory '{}': {}", parent.display(), e))
        })?;

        // Construct the theoretical canonical path
        canonical_parent.join(path.file_name().unwrap_or_default())
    };

    if !canonical_path.starts_with(&canonical_root) {
        return Err(ToolError::PermissionDenied(format!(
            "Path '{}' is outside workspace root '{}'",
            path.display(),
            workspace_root.display()
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_read_file() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "hello world").unwrap();

        let tool = ReadFileTool;
        let ctx = ToolContext {
            workspace_root: dir.path().to_path_buf(),
            session_id: "test".to_string(),
        };

        let result = tool
            .execute(serde_json::json!({ "path": "test.txt" }), &ctx)
            .await
            .unwrap();
        assert_eq!(result.content, "hello world");
    }

    #[tokio::test]
    async fn test_read_file_not_found() {
        let dir = TempDir::new().unwrap();
        let tool = ReadFileTool;
        let ctx = ToolContext {
            workspace_root: dir.path().to_path_buf(),
            session_id: "test".to_string(),
        };

        let result = tool
            .execute(serde_json::json!({ "path": "nonexistent.txt" }), &ctx)
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_path_relative() {
        let root = Path::new("/workspace");
        assert_eq!(resolve_path("foo.txt", root), PathBuf::from("/workspace/foo.txt"));
    }

    #[test]
    fn test_resolve_path_absolute() {
        let root = Path::new("/workspace");
        assert_eq!(resolve_path("/etc/passwd", root), PathBuf::from("/etc/passwd"));
    }
}
