use anyhow::{Context, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use crate::kernel::{Kernel, BedrockConfig};
use crate::tools::registry::ToolRegistry;
use crate::tools::builtins::create_default_registry;
use crate::persistence::state::StateStore;
use crate::inference::embeddings::EmbeddingProvider;
use crate::inference::provider::ProviderClient;

/// Builder for constructing a `Kernel` instance.
pub struct RuntimeBuilder {
    config: BedrockConfig,
    json: bool,
    tool_registry: ToolRegistry,
    state: Option<StateStore>,
    embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
}

impl RuntimeBuilder {
    /// Create a new builder with the given configuration.
    pub fn new(config: BedrockConfig) -> Self {
        Self {
            config,
            json: false,
            tool_registry: create_default_registry(),
            state: None,
            embedding_provider: None,
        }
    }

    /// Enable JSON output mode (NDJSON).
    pub fn json_mode(mut self, json: bool) -> Self {
        self.json = json;
        self
    }

    /// Set a custom state store.
    pub fn with_state_store(mut self, state: StateStore) -> Self {
        self.state = Some(state);
        self
    }

    /// Register a custom tool registry (overwriting defaults).
    pub fn with_tool_registry(mut self, registry: ToolRegistry) -> Self {
        self.tool_registry = registry;
        self
    }

    /// Build the Kernel.
    pub fn build(self) -> Result<Kernel> {
        Ok(Kernel {
            config: self.config,
            json: self.json,
            tool_registry: self.tool_registry,
            state: self.state,
            harness: Arc::new(Mutex::new(None)),
            check_watcher: None,
            clients: HashMap::new(),
            embedding_provider: self.embedding_provider,
            active_queue: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            mcp_clients: Vec::new(),
        })
    }
}
