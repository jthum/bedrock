use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::tools::{parse_args, Tool, ToolContext, ToolError, ToolOutput};

pub struct SubmitTaskTool;

#[derive(Deserialize)]
struct SubmitTaskArgs {
    /// Title of the task/plan
    title: String,
    /// List of subtasks to execute
    subtasks: Vec<String>,
    /// Whether to clear the existing queue before adding these tasks
    #[serde(default)]
    clear_existing: bool,
}

#[async_trait]
impl Tool for SubmitTaskTool {
    fn name(&self) -> &str {
        "submit_task"
    }

    fn description(&self) -> &str {
        "Submit a breakdown of tasks to be executed. Use this to plan complex workflows."
    }

    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Title of the overall task or plan"
                },
                "subtasks": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "List of subtasks to execute in order"
                },
                "clear_existing": {
                    "type": "boolean",
                    "description": "If true, clears the current queue before adding these tasks. Default is false."
                }
            },
            "required": ["title", "subtasks"]
        })
    }

    async fn execute(&self, params: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: SubmitTaskArgs = parse_args(params)?;
        
        // We just return the details. The Kernel will handle the actual queue manipulation
        // by inspecting the metadata.
        Ok(ToolOutput {
            content: format!("Task '{}' submitted with {} subtasks.", args.title, args.subtasks.len()),
            metadata: serde_json::json!({
                "action": "submit_task",
                "title": args.title,
                "subtasks": args.subtasks,
                "clear_existing": args.clear_existing
            }),
        })
    }
}
