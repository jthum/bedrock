use anyhow::Result;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::tools::{Tool, ToolContext, ToolError, ToolOutput};
use mcp_sdk::client::McpClient;
use mcp_sdk::transport::StdioTransport;
use mcp_sdk::types::ToolDefinition;

/// The builtin tool that allows agents to request an MCP server connection.
pub struct BridgeMcp;

#[async_trait]
impl Tool for BridgeMcp {
    fn name(&self) -> &str {
        "bridge_mcp"
    }

    fn description(&self) -> &str {
        "Connect to a Model Context Protocol (MCP) server to dynamically load its tools. Provide the command and arguments to spawn the server (e.g., `['npx', '-y', '@modelcontextprotocol/server-filesystem', '/path']`)."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The executable command (e.g., 'npx', 'python', '/bin/my-server')"
                },
                "args": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Arguments for the command"
                }
            },
            "required": ["command", "args"]
        })
    }

    async fn execute(&self, params: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let command = params["command"].as_str()
            .ok_or_else(|| ToolError::InvalidParams("Missing 'command'".to_string()))?
            .to_string();
        
        let args: Vec<String> = params["args"].as_array()
            .ok_or_else(|| ToolError::InvalidParams("Missing 'args' array".to_string()))?
            .iter()
            .map(|v| v.as_str().unwrap_or_default().to_string())
            .collect();

        // We don't spawn here. We return metadata so the Kernel can spawn and register tools.
        // This avoids needing to inject the ToolRegistry into this tool.
        let mut output = ToolOutput::new(format!("Requesting MCP connection to: {} {:?}", command, args));
        output.metadata["action"] = json!("spawn_mcp");
        output.metadata["command"] = json!(command);
        output.metadata["args"] = json!(args);

        Ok(output)
    }
}

/// A proxy tool that forwards calls to a remote MCP server.
pub struct McpToolProxy {
    client: Arc<McpClient<StdioTransport>>,
    def: ToolDefinition,
}

impl McpToolProxy {
    pub fn new(client: Arc<McpClient<StdioTransport>>, def: ToolDefinition) -> Self {
        Self { client, def }
    }
}

#[async_trait]
impl Tool for McpToolProxy {
    fn name(&self) -> &str {
        &self.def.name
    }

    fn description(&self) -> &str {
        self.def.description.as_deref().unwrap_or("MCP Tool")
    }

    fn parameters_schema(&self) -> Value {
        self.def.input_schema.clone()
    }

    async fn execute(&self, params: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let result = self.client.call_tool(&self.def.name, params).await
            .map_err(|e| ToolError::ExecutionError(format!("MCP Call Failed: {}", e)))?;

        // Convert MCP content to ToolOutput text
        let text_output = result.as_text();

        if result.is_error {
            return Err(ToolError::ExecutionError(text_output));
        }

        Ok(ToolOutput::new(text_output.trim().to_string()))
    }
}
