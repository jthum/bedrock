use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::tools::{parse_args, Tool, ToolContext, ToolError, ToolOutput};
use super::read_file::{resolve_path, validate_workspace_path};

pub struct EditFileTool;

#[derive(Deserialize)]
struct EditFileArgs {
    /// Path to edit (relative to workspace root, or absolute)
    path: String,
    /// Exact string to search for in the file
    old_text: String,
    /// Replacement string
    new_text: String,
}

#[async_trait]
impl Tool for EditFileTool {
    fn name(&self) -> &str {
        "edit_file"
    }

    fn description(&self) -> &str {
        "Edit a file by replacing an exact string match. The old_text must appear exactly once in the file. Use read_file first to see the current contents."
    }

    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the file to edit"
                },
                "old_text": {
                    "type": "string",
                    "description": "Exact text to find and replace (must match exactly once)"
                },
                "new_text": {
                    "type": "string",
                    "description": "Replacement text"
                }
            },
            "required": ["path", "old_text", "new_text"]
        })
    }

    async fn execute(&self, params: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: EditFileArgs = parse_args(params)?;
        let path = resolve_path(&args.path, &ctx.workspace_root);

        validate_workspace_path(&path, &ctx.workspace_root)?;

        let content = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| ToolError::ExecutionError(format!("Failed to read {}: {}", path.display(), e)))?;

        // Count occurrences
        let count = content.matches(&args.old_text).count();
        if count == 0 {
            return Err(ToolError::ExecutionError(format!(
                "old_text not found in {}. Use read_file to verify the current contents.",
                path.display()
            )));
        }
        if count > 1 {
            return Err(ToolError::ExecutionError(format!(
                "old_text found {} times in {} — it must appear exactly once. Use a more specific match.",
                count,
                path.display()
            )));
        }

        let new_content = content.replacen(&args.old_text, &args.new_text, 1);

        tokio::fs::write(&path, &new_content).await.map_err(|e| {
            ToolError::ExecutionError(format!("Failed to write {}: {}", path.display(), e))
        })?;

        Ok(ToolOutput {
            content: format!(
                "Successfully edited {}. Replaced {} bytes with {} bytes.",
                path.display(),
                args.old_text.len(),
                args.new_text.len()
            ),
            metadata: serde_json::json!({
                "path": path.display().to_string(),
                "old_len": args.old_text.len(),
                "new_len": args.new_text.len(),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_edit_file() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "hello world").unwrap();

        let tool = EditFileTool;
        let ctx = ToolContext {
            workspace_root: dir.path().to_path_buf(),
            session_id: "test".to_string(),
        };

        let result = tool
            .execute(
                serde_json::json!({
                    "path": "test.txt",
                    "old_text": "world",
                    "new_text": "rust"
                }),
                &ctx,
            )
            .await
            .unwrap();

        assert!(result.content.contains("Successfully edited"));
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "hello rust");
    }

    #[tokio::test]
    async fn test_edit_file_not_found_text() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "hello world").unwrap();

        let tool = EditFileTool;
        let ctx = ToolContext {
            workspace_root: dir.path().to_path_buf(),
            session_id: "test".to_string(),
        };

        let result = tool
            .execute(
                serde_json::json!({
                    "path": "test.txt",
                    "old_text": "nonexistent",
                    "new_text": "rust"
                }),
                &ctx,
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_edit_file_multiple_matches() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "aaa aaa aaa").unwrap();

        let tool = EditFileTool;
        let ctx = ToolContext {
            workspace_root: dir.path().to_path_buf(),
            session_id: "test".to_string(),
        };

        let result = tool
            .execute(
                serde_json::json!({
                    "path": "test.txt",
                    "old_text": "aaa",
                    "new_text": "bbb"
                }),
                &ctx,
            )
            .await;

        assert!(result.is_err());
        // Original file should be unchanged
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "aaa aaa aaa");
    }
}
