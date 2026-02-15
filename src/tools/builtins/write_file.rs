use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::tools::{parse_args, Tool, ToolContext, ToolError, ToolOutput};
use super::read_file::{resolve_path, validate_workspace_path};

pub struct WriteFileTool;

#[derive(Deserialize)]
struct WriteFileArgs {
    /// Path to write (relative to workspace root, or absolute)
    path: String,
    /// Content to write to the file
    content: String,
}

#[async_trait]
impl Tool for WriteFileTool {
    fn name(&self) -> &str {
        "write_file"
    }

    fn description(&self) -> &str {
        "Create a new file or overwrite an existing file with the given content. Creates parent directories if needed."
    }

    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the file to write (relative to workspace root, or absolute)"
                },
                "content": {
                    "type": "string",
                    "description": "Content to write to the file"
                }
            },
            "required": ["path", "content"]
        })
    }

    #[tracing::instrument(skip(self, params, ctx), fields(path = %params["path"].as_str().unwrap_or("unknown")))]
    async fn execute(&self, params: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: WriteFileArgs = parse_args(params)?;
        tracing::info!(path = %args.path, "Writing file");
        let path = resolve_path(&args.path, &ctx.workspace_root);

        // Security: validate path is within workspace
        validate_workspace_path(&path, &ctx.workspace_root)?;

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ToolError::ExecutionError(format!(
                    "Failed to create directories for {}: {}",
                    path.display(),
                    e
                ))
            })?;
        }

        let bytes = args.content.len();
        tokio::fs::write(&path, &args.content).await.map_err(|e| {
            ToolError::ExecutionError(format!("Failed to write {}: {}", path.display(), e))
        })?;

        Ok(ToolOutput {
            content: format!("Successfully wrote {} bytes to {}", bytes, path.display()),
            metadata: serde_json::json!({
                "path": path.display().to_string(),
                "bytes": bytes,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_write_file() {
        let dir = TempDir::new().unwrap();
        let tool = WriteFileTool;
        let ctx = ToolContext {
            workspace_root: dir.path().to_path_buf(),
            session_id: "test".to_string(),
        };

        let result = tool
            .execute(
                serde_json::json!({ "path": "output.txt", "content": "hello!" }),
                &ctx,
            )
            .await
            .unwrap();

        assert!(result.content.contains("6 bytes"));
        let written = std::fs::read_to_string(dir.path().join("output.txt")).unwrap();
        assert_eq!(written, "hello!");
    }

    #[tokio::test]
    async fn test_write_file_creates_dirs() {
        let dir = TempDir::new().unwrap();
        let tool = WriteFileTool;
        let ctx = ToolContext {
            workspace_root: dir.path().to_path_buf(),
            session_id: "test".to_string(),
        };

        tool.execute(
            serde_json::json!({ "path": "deep/nested/file.txt", "content": "deep" }),
            &ctx,
        )
        .await
        .unwrap();

        let written = std::fs::read_to_string(dir.path().join("deep/nested/file.txt")).unwrap();
        assert_eq!(written, "deep");
    }
}
