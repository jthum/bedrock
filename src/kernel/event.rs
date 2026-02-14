use serde::Serialize;

/// Every action in Bedrock produces a typed `KernelEvent`.
///
/// Events are:
/// 1. **Typed** — Each event has a specific variant
/// 2. **Persisted** — Written to libSQL for auditability (Phase 3)
/// 3. **Harness-gated** — Certain events pass through harness hooks before execution (Phase 4)
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KernelEvent {
    /// Agent session begins
    AgentStart {
        session_id: String,
    },

    /// Agent session completes
    AgentEnd {
        message_count: u32,
        total_input_tokens: u32,
        total_output_tokens: u32,
    },

    /// New LLM call begins
    TurnStart {
        turn_index: u32,
    },

    /// LLM call completes
    TurnEnd {
        turn_index: u32,
        has_tool_calls: bool,
    },

    /// Streaming message begins
    MessageStart {
        role: String,
        model: String,
    },

    /// Streaming text chunk received
    MessageDelta {
        content_delta: String,
    },

    /// Streaming thinking chunk received
    ThinkingDelta {
        thinking: String,
    },

    /// Complete message assembled
    MessageEnd {
        role: String,
        input_tokens: u32,
        output_tokens: u32,
    },

    /// LLM requests a tool execution
    ToolCall {
        id: String,
        name: String,
        args: serde_json::Value,
    },

    /// Tool execution completed
    ToolResult {
        id: String,
        output: String,
        is_error: bool,
    },

    /// Tool execution begins (for logging/timing)
    ToolExecStart {
        id: String,
        name: String,
    },

    /// Tool execution completes
    ToolExecEnd {
        id: String,
        success: bool,
    },

    /// Token/cost accounting update
    TokenUsage {
        input_tokens: u32,
        output_tokens: u32,
        cost_usd: f64,
    },

    /// Harness engine rejected an action
    HarnessRejection {
        /// Which event type was rejected (e.g., "tool_call")
        event: String,
        /// Human-readable reason from the harness script
        reason: String,
    },
}

impl KernelEvent {
    /// Get the event type name as a string.
    pub fn event_type(&self) -> &'static str {
        match self {
            KernelEvent::AgentStart { .. } => "agent_start",
            KernelEvent::AgentEnd { .. } => "agent_end",
            KernelEvent::TurnStart { .. } => "turn_start",
            KernelEvent::TurnEnd { .. } => "turn_end",
            KernelEvent::MessageStart { .. } => "message_start",
            KernelEvent::MessageDelta { .. } => "message_delta",
            KernelEvent::ThinkingDelta { .. } => "thinking_delta",
            KernelEvent::MessageEnd { .. } => "message_end",
            KernelEvent::ToolCall { .. } => "tool_call",
            KernelEvent::ToolResult { .. } => "tool_result",
            KernelEvent::ToolExecStart { .. } => "tool_exec_start",
            KernelEvent::ToolExecEnd { .. } => "tool_exec_end",
            KernelEvent::TokenUsage { .. } => "token_usage",
            KernelEvent::HarnessRejection { .. } => "harness_rejection",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_serialization() {
        let event = KernelEvent::AgentStart {
            session_id: "test-123".to_string(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"agent_start\""));
        assert!(json.contains("\"session_id\":\"test-123\""));
    }

    #[test]
    fn test_event_type_names() {
        assert_eq!(
            KernelEvent::AgentStart {
                session_id: "x".into()
            }
            .event_type(),
            "agent_start"
        );
        assert_eq!(
            KernelEvent::HarnessRejection {
                event: "tool_call".into(),
                reason: "blocked".into()
            }
            .event_type(),
            "harness_rejection"
        );
    }

    #[test]
    fn test_tool_call_event_serialization() {
        let event = KernelEvent::ToolCall {
            id: "call_1".to_string(),
            name: "read_file".to_string(),
            args: serde_json::json!({ "path": "main.rs" }),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"tool_call\""));
        assert!(json.contains("\"name\":\"read_file\""));
        assert!(json.contains("\"path\":\"main.rs\""));
    }
}
