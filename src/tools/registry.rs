use std::collections::BTreeMap;

use super::{Tool, ToolContext};

/// Central registry of available tools.
///
/// The ToolRegistry owns all tool instances and provides:
/// - Tool lookup by name
/// - JSON schema generation for LLM tool definitions
/// - Tool execution dispatch
pub struct ToolRegistry {
    tools: BTreeMap<String, Box<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: BTreeMap::new(),
        }
    }

    /// Register a tool. Returns error if a tool with the same name is already registered.
    pub fn register(&mut self, tool: Box<dyn Tool>) -> anyhow::Result<()> {
        let name = tool.name().to_string();
        if self.tools.contains_key(&name) {
            anyhow::bail!("Tool '{}' already registered", name);
        }
        self.tools.insert(name, tool);
        Ok(())
    }

    /// Get a tool by name.
    pub fn get(&self, name: &str) -> Option<&dyn Tool> {
        self.tools.get(name).map(|t| t.as_ref())
    }

    /// Generate JSON tool definitions for the LLM API.
    ///
    /// Returns a Vec of tool definition objects matching the standard format:
    /// ```json
    /// { "name": "...", "description": "...", "input_schema": { ... } }
    /// ```
    pub fn tool_definitions(&self) -> Vec<serde_json::Value> {
        self.tools
            .values()
            .map(|tool| {
                serde_json::json!({
                    "name": tool.name(),
                    "description": tool.description(),
                    "input_schema": tool.parameters_schema(),
                })
            })
            .collect()
    }

    /// Execute a tool by name with the given arguments.
    pub async fn execute(
        &self,
        name: &str,
        args: serde_json::Value,
        ctx: &ToolContext,
    ) -> Result<super::ToolOutput, super::ToolError> {
        let tool = self
            .get(name)
            .ok_or_else(|| super::ToolError::ExecutionError(format!("Unknown tool: {}", name)))?;
        tool.execute(args, ctx).await
    }

    /// Get the number of registered tools.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Returns true if no tools are registered.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::builtins;

    #[test]
    fn test_registry_register_and_get() {
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(builtins::ReadFileTool)).unwrap();
        assert!(registry.get("read_file").is_some());
        assert!(registry.get("nonexistent").is_none());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_tool_definitions() {
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(builtins::ReadFileTool)).unwrap();
        registry.register(Box::new(builtins::WriteFileTool)).unwrap();

        let defs = registry.tool_definitions();
        assert_eq!(defs.len(), 2);

        // Check that each definition has the required fields
        for def in &defs {
            assert!(def.get("name").is_some());
            assert!(def.get("description").is_some());
            assert!(def.get("input_schema").is_some());
        }
    }

    #[test]
    #[should_panic(expected = "already registered")]
    fn test_duplicate_registration_panics() {
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(builtins::ReadFileTool)).unwrap();
        registry.register(Box::new(builtins::ReadFileTool)).unwrap(); // should panic
    }
}
