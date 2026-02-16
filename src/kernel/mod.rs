pub mod config;
pub mod event;
pub mod builder;
pub mod session;

use anyhow::{Context, Result};
use builder::RuntimeBuilder;
use session::SessionState;
use config::BedrockConfig;
use event::KernelEvent;
use futures::StreamExt;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{info, warn, error, debug, instrument};
use futures::future::join_all;
use std::collections::{HashMap, VecDeque};

use crate::harness::engine::HarnessEngine;
use crate::harness::globals::HarnessAppData;
use crate::harness::context::ContextWrapper;
use crate::harness::verdict::Verdict;
use crate::inference::provider::{
    self, InferenceContent, InferenceMessage, InferenceRole, ProviderClient, ProviderKind,
};
use crate::persistence::state::StateStore;
use crate::tools::ToolContext;
use crate::tools::builtins::create_default_registry;
use crate::tools::registry::ToolRegistry;
use crate::tools::mcp::McpToolProxy;
use mcp_sdk::client::McpClient;
use mcp_sdk::transport::StdioTransport;
use crate::inference::embeddings::EmbeddingProvider;
use notify::{RecommendedWatcher, Event};

/// The Bedrock Kernel — manages the agent loop, event system, and tool execution.
///
/// The Kernel has no opinions about agent behavior. It provides the physics:
/// transport, streaming, tool execution, persistence, and event hooks.
/// Harness scripts define the behavior.
pub struct Kernel {
    pub(crate) config: BedrockConfig,
    pub(crate) json: bool,
    pub(crate) tool_registry: ToolRegistry,
    pub(crate) state: Option<StateStore>,
    /// Thread-safe harness engine for hot-reloading
    pub(crate) harness: Arc<Mutex<Option<HarnessEngine>>>,
    /// Watcher handle to keep it alive
    pub(crate) check_watcher: Option<RecommendedWatcher>,
    pub(crate) clients: HashMap<String, ProviderClient>,
    pub(crate) embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    /// Active session queue for harness interaction
    pub(crate) active_queue: crate::harness::globals::ActiveSessionQueue,
    pub(crate) mcp_clients: Vec<Arc<McpClient>>,
}

/// A pending tool call collected during streaming.
#[derive(Debug, Clone)]
struct PendingToolCall {
    id: String,
    name: String,
    args: serde_json::Value,
}

impl Kernel {
    /// Create a new Kernel with the given configuration.
    /// Create a new builder for Kernel.
    pub fn builder(config: BedrockConfig) -> RuntimeBuilder {
        RuntimeBuilder::new(config)
    }

    /// Create a new Kernel with the given configuration.
    /// DEPRECATED: Use `Kernel::builder(config).build()` instead.
    #[deprecated(since = "0.9.0", note = "Use Kernel::builder() instead")]
    pub fn new(config: BedrockConfig, json: bool) -> Self {
        Self {
            config,
            json,
            tool_registry: crate::tools::builtins::create_default_registry(),
            state: None,
            harness: Arc::new(Mutex::new(None)),
            check_watcher: None,
            clients: HashMap::new(),
            embedding_provider: None,
            active_queue: Arc::new(Mutex::new(None)),
            mcp_clients: Vec::new(),
        }
    }

    /// Create a new session.
    pub fn create_session(&self) -> SessionState {
        let mut session = SessionState::new();
        // Spawn background persistence if state is available
        if let Some(ref store) = self.state {
             let mut rx_opt = session.event_rx.take(); // take the rx from session
             if let Some(rx_mutex) = rx_opt {
                 // extract rx from mutex? It's Arc<Mutex<Option<Rx>>>.
                 // We need to spawn a task that locks it once or takes it.
                 // SessionState::new puts Some(rx) in.
                 // We want to move rx into the task.
                 // But rx_mutex is Arc<Mutex...>. We can't move out of Arc.
                 // Wait, SessionState definition: 
                 // pub event_rx: Option<Arc<Mutex<Option<mpsc::UnboundedReceiver<(String, KernelEvent)>>>>>,
                 // We can take the Arc, then lock and take the Option.
                 let store_clone = store.clone();
                 // We need to spawn a task.
                 // But we can't block here.
                 // We can spawn a task that locks and runs.
                 let rx_arc = rx_mutex;
                 let handle = tokio::spawn(async move {
                    let mut rx_guard = rx_arc.lock().await;
                     if let Some(mut rx) = rx_guard.take() {
                         drop(rx_guard); // release lock
                         while let Some((session_id, event)) = rx.recv().await {
                             let event_type = event.event_type().to_string();
                             let payload = serde_json::to_value(&event).unwrap_or_default();
                             if let Err(e) = store_clone.insert_event(&session_id, &event_type, &payload).await {
                                 warn!(error = %e, "Background persistence error");
                             }
                         }
                     }
                 });
                 session.event_task = Some(Arc::new(Mutex::new(Some(handle))));
             }
        }
        session
    }

    /// Initialize all configured provider clients. Call before `init_harness()` and `run()`.
    pub fn init_clients(&mut self) -> Result<()> {
        for (name, config) in &self.config.providers {
            let client = self.create_client(name, config)?;
            self.clients.insert(name.clone(), client);
        }
        
        // Ensure the default provider is available
        let default_provider_name = &self.config.agent.provider;
        
        if !self.clients.contains_key(default_provider_name) {
             // Try to init if it's in the config but failed? Or if it wasn't in the loop?
             // The loop covers all in `self.config.providers`. 
             // If default provider is NOT in `self.config.providers`, we can't init it unless it's a known implicit type.
             // But we moved away from implicit.
             // So we just error if it's missing.
             if !self.config.providers.contains_key(default_provider_name) {
                 anyhow::bail!("Default provider '{}' not found in [providers] configuration", default_provider_name);
             }
        }

        // Initialize embedding provider
        // WE need to find the openai provider config if selected.
        let embedding_provider = if let Some(ref config) = self.config.embeddings {
            match config {
                crate::kernel::config::EmbeddingConfig::OpenAI => {
                     // Find a provider with type="openai"
                     let openai_config = self.config.providers.values()
                        .find(|p| p.kind == "openai")
                        .with_context(|| "OpenAI embeddings selected but no OpenAI provider configured")?;
                        
                     crate::inference::embeddings::create_embedding_provider(&crate::inference::embeddings::EmbeddingConfig::OpenAI {
                        api_key: openai_config.api_key_env.as_ref().map(|k| std::env::var(k).context(format!("Environment variable '{}' not set", k)).unwrap()).unwrap(),
                        model: "text-embedding-3-small".to_string(),
                    })
                },
                crate::kernel::config::EmbeddingConfig::NoOp => {
                    crate::inference::embeddings::create_embedding_provider(&crate::inference::embeddings::EmbeddingConfig::NoOp)
                }
            }
        } else {
             // Default logic: if there is an openai provider, use it?
             if let Some(openai_config) = self.config.providers.values().find(|p| p.kind == "openai") {
                 crate::inference::embeddings::create_embedding_provider(&crate::inference::embeddings::EmbeddingConfig::OpenAI {
                     api_key: openai_config.api_key_env.as_ref().map(|k| std::env::var(k).unwrap_or_default()).unwrap_or_default(),
                     model: "text-embedding-3-small".to_string(),
                 })
             } else {
                 crate::inference::embeddings::create_embedding_provider(&crate::inference::embeddings::EmbeddingConfig::NoOp)
             }
        };
        
        self.embedding_provider = Some(Arc::from(embedding_provider));

        Ok(())
    }

    /// Initialize the state store. Call before `run()`.
    pub async fn init_state(&mut self) -> Result<()> {
        let db_path = &self.config.persistence.database_path;
        let store = StateStore::open(db_path).await.with_context(|| {
            format!("Failed to initialize state store at '{}'", db_path)
        })?;
        info!(db_path = %db_path, "State store initialized");
        self.state = Some(store.clone());

        // Start background persistence task - MOVED to create_session
        // init_state now only initializes the store.
        
        Ok(())
    }

        Ok(())
    }

    /// Initialize the harness engine. Call after `init_state()` and before `run()`.
    #[instrument(skip(self), fields(directory = %self.config.harness.directory))]
    pub async fn init_harness(&mut self) -> Result<()> {
        info!("Initializing harness");
        let harness_dir = PathBuf::from(&self.config.harness.directory);

        // Resolve fs_root: "." means workspace root, otherwise use as-is
        let fs_root = if self.config.harness.fs_root == "." {
            PathBuf::from(&self.config.kernel.workspace_root)
        } else {
            PathBuf::from(&self.config.harness.fs_root)
        };

        let app_data = HarnessAppData {
            fs_root,
            workspace_root: PathBuf::from(&self.config.kernel.workspace_root),
            state_store: self.state.clone(),
            clients: self.clients.clone(),
            embedding_provider: self.embedding_provider.clone(),
            queue: self.active_queue.clone(),
            config: Arc::new(self.config.clone()),
        };

        let mut engine = HarnessEngine::new(app_data)
            .with_context(|| "Failed to create harness engine")?;

        engine.load_dir(&harness_dir)
            .with_context(|| format!("Failed to load harness scripts from '{}'", harness_dir.display()))?;

        let script_count = engine.loaded_scripts().len();
        if script_count > 0 {
            info!(count = script_count, directory = %harness_dir.display(), "Harness scripts loaded");
            for name in engine.loaded_scripts() {
                debug!(script = %name, "Loaded harness script");
            }
        } else {
            warn!(directory = %harness_dir.display(), "No harness scripts found");
        }

        {
            let mut h = self.harness.lock().await;
            *h = Some(engine);
        }
        Ok(())
    }

    /// Reload the harness from disk (atomic swap).
    #[instrument(skip(self))]
    pub async fn reload_harness(&mut self) -> Result<()> {
        info!("Reloading harness");
        self.init_harness().await
    }

    #[instrument(skip_all)]
    pub async fn reload_harness_static(
        harness: Arc<Mutex<Option<HarnessEngine>>>,
        config: BedrockConfig,
        clients: HashMap<String, ProviderClient>,
        state: Option<StateStore>,
        embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
        embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
        active_queue: crate::harness::globals::ActiveSessionQueue,
    ) -> Result<()> {
        let harness_dir = PathBuf::from(&config.harness.directory);
        let fs_root = if config.harness.fs_root == "." {
            PathBuf::from(&config.kernel.workspace_root)
        } else {
            PathBuf::from(&config.harness.fs_root)
        };

        let app_data = HarnessAppData {
            fs_root,
            workspace_root: PathBuf::from(&config.kernel.workspace_root),
            state_store: state.clone(),
            clients,
            embedding_provider,
            embedding_provider,
            queue: active_queue,
            config: Arc::new(config),
        };

        match HarnessEngine::new(app_data) {
            Ok(mut engine) => {
                match engine.load_dir(&harness_dir) {
                    Ok(_) => {
                        let script_count = engine.loaded_scripts().len();
                        let mut h = harness.lock().await;
                        *h = Some(engine);
                        info!(count = script_count, "Harness reloaded successfully");
                    }
                    Err(e) => error!(error = %e, "Failed to load harness scripts"),
                }
            }
            Err(e) => error!(error = %e, "Failed to create harness engine during static reload"),
        }
        Ok(())
    }

    /// Start watching the harness directory for changes (background thread).
    #[instrument(skip(self))]
    pub fn start_watcher(&mut self) -> Result<()> {
        use notify::{RecursiveMode, Watcher};
        use std::time::Duration;

        let harness_clone = self.harness.clone();
        let config_clone = self.config.clone();
        let clients_clone = self.clients.clone();
        let state_clone = self.state.clone();
        let embedding_clone = self.embedding_provider.clone();
        let queue_clone = self.active_queue.clone();
        let harness_dir = PathBuf::from(&config_clone.harness.directory);

        if !harness_dir.exists() {
            warn!(directory = %harness_dir.display(), "Harness directory does not exist, skipping watcher");
            return Ok(());
        }

        // We use an async channel to debounce events
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(10);

        // Spawn background task to handle reloads with debouncing
        tokio::spawn(async move {
            while let Some(_) = rx.recv().await {
                // Debounce: Wait for more events
                tokio::time::sleep(Duration::from_millis(200)).await;
                // Clear any pending events that arrived during sleep
                while let Ok(_) = rx.try_recv() {}

                info!("Hot-reload triggered by file change");
                let h = harness_clone.clone();
                let c = config_clone.clone();
                let cl = clients_clone.clone();
                let s = state_clone.clone();
                let e = embedding_clone.clone();
                let q = queue_clone.clone();
                
                tokio::spawn(async move {
                    if let Err(err) = Self::reload_harness_static(h, c, cl, s, e, q).await {
                        error!(error = %err, "Harness hot-reload failed");
                    }
                });
            }
        });

        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            match res {
                Ok(event) => {
                    if event.kind.is_modify() || event.kind.is_create() || event.kind.is_remove() {
                        let _ = tx.blocking_send(());
                    }
                }
                Err(e) => error!(error = ?e, "Watcher channel error"),
            }
        })?;

        watcher.watch(&harness_dir, RecursiveMode::NonRecursive)?;
        self.check_watcher = Some(watcher);

        info!(directory = %harness_dir.display(), "Watching harness directory");

        Ok(())
    }

    /// Run a Lua script directly in the harness (for testing/verification).
    pub async fn run_script(&self, script: &str) -> Result<()> {
        let mut harness_lock = self.harness.lock().await;
        if let Some(ref mut engine) = *harness_lock {
             engine.load_script_str(script)?;
        } else {
            anyhow::bail!("Harness not initialized");
        }
        Ok(())
    }

    /// Run the agent loop with the given prompt.
    ///
    /// Run the agent with multiple turns.
    /// Run the agent with multiple turns.
    #[instrument(skip(self, session), fields(session_id = %session.id))]
    pub async fn run(&mut self, session: &mut SessionState, prompt: Option<String>) -> Result<()> {
        // Set active queue for harness
        {
            let mut aq = self.active_queue.lock().await;
            *aq = Some(session.queue.clone());
        }

        if let Some(p) = prompt {
            session.queue.lock().await.push_back(p);
        }

        let session_id = session.id.clone();
        
        // Resume session if history is not empty? 
        // For now, if turn_index is 0, we treat it as start.
        if session.turn_index == 0 {
            info!(session_id = %session_id, "Starting new agent session");
            self.persist_event(session, &KernelEvent::AgentStart {
                session_id: session_id.clone(),
            });

            {
                let harness = self.harness.lock().await;
                if let Some(ref engine) = *harness {
                    if let Err(e) = engine.on_agent_start(&session_id) {
                         warn!(error = %e, "Harness on_agent_start failed");
                    }
                }
            }
        }

        loop {
            // Pop next task
            {
                let mut q = session.queue.lock().await;
                if q.is_empty() {
                    debug!("Queue empty, ending run");
                    break;
                }
                let task = q.pop_front().unwrap();
                drop(q);
                
                info!(task = %task, "Running task");
                self.run_task(session, &task).await?;
            }
            
            // ─── Harness Hook: on_task_complete ─────────────────────
            // Triggered when the queue is explicitly empty.
            let mut recheck = false;
            
            let verdict_result = {
                let harness = self.harness.lock().await;
                if let Some(ref engine) = *harness {
                    let payload = serde_json::json!({
                        "session_id": session.id,
                        "turn_count": session.turn_index,
                    });
                     Some(engine.evaluate("on_task_complete", payload))
                } else {
                    None
                }
            };

            if let Some(result) = verdict_result {
                match result {
                    Ok(Verdict::Modify(new_tasks_val)) => {
                        if let Some(new_tasks) = new_tasks_val.as_array() {
                            if !new_tasks.is_empty() {
                                let mut q = session.queue.lock().await;
                                for task in new_tasks {
                                    if let Some(t) = task.as_str() {
                                        q.push_back(t.to_string());
                                    }
                                }
                                info!(count = new_tasks.len(), "Validation failed or extended by harness; new tasks queued");
                                recheck = true;
                            }
                        }
                    },
                    Ok(Verdict::Reject(reason)) => {
                        warn!(reason = %reason, "Session ended with REJECTION from harness");
                        break;
                    },
                    Ok(_) => {},
                    Err(e) => {
                        warn!(error = %e, "Harness on_task_complete error");
                    }
                }
            }
            
            if recheck {
                continue;
            }
            break;
        }
        
        Ok(())
    }

    /// End the session and emit AgentEnd event.
    pub async fn end_session(&mut self, session: &mut SessionState) -> Result<()> {
         self.persist_event(session, &KernelEvent::AgentEnd {
            message_count: session.turn_index,
            total_input_tokens: session.total_input_tokens,
            total_output_tokens: session.total_output_tokens,
         });
         
         // Clear active queue
         {
             let mut aq = self.active_queue.lock().await;
             *aq = None;
         }
         
         Ok(())
    }

    /// Add a prompt to the end of the queue.
    pub async fn queue_prompt(&self, session: &SessionState, prompt: String) {
        let mut q = session.queue.lock().await;
        q.push_back(prompt);
    }
    
    /// Execute a single task (one specific prompt) within the persistent session.
    #[instrument(skip(self, session, prompt), fields(task = %prompt))]
    async fn run_task(&mut self, session: &mut SessionState, prompt: &str) -> Result<()> {
        let session_id = session.id.clone();

        // Append user message to history
        session.history.push(InferenceMessage {
            role: InferenceRole::User,
            content: vec![InferenceContent::Text { text: prompt.to_string() }],
            tool_call_id: None,
        });

        let tool_ctx = ToolContext {
            workspace_root: std::path::PathBuf::from(&self.config.kernel.workspace_root),
            session_id: session_id.clone(),
        };

        // Persist user message
        if let Some(ref store) = self.state {
            let _ = store.insert_message(
                &session_id,
                session.turn_index,
                "user",
                &serde_json::json!([{"type": "text", "text": prompt}]),
                None,
            ).await;
        }

        let mut task_turn_count = 0;
        let max_task_turns = self.config.kernel.max_turns;

        loop {
            if task_turn_count >= max_task_turns {
                error!(max_turns = max_task_turns, "Max turns reached for this task");
                break;
            }

            if !self.execute_turn(session, &tool_ctx).await? {
                break;
            }

            self.evaluate_token_usage(session.total_input_tokens, session.total_output_tokens).await;
            session.turn_index += 1;
            task_turn_count += 1;
        }
        Ok(())
    }

    /// Execute a single turn of the agent loop. Returns true if loop should continue.
    async fn execute_turn(&mut self, session: &mut SessionState, tool_ctx: &ToolContext) -> Result<bool> {
        let session_id = session.id.clone();

        // Turn-local configuration
        let mut model = self.config.agent.model.clone();
        let mut provider_name = self.config.agent.provider.clone();
        let mut system_prompt = self.config.agent.system_prompt.clone();

        self.persist_event(session, &KernelEvent::TurnStart { turn_index: session.turn_index });

        // ─── Harness Hook: on_before_inference ───────────────────────
        let mut thinking_budget = self.config.agent.thinking.as_ref()
            .and_then(|t| if t.enabled { t.budget_tokens } else { None })
            .unwrap_or(0);

        {
            let harness = self.harness.lock().await;
            if let Some(ref engine) = *harness {
                let ctx = ContextWrapper::new(
                    model.clone(),
                    provider_name.clone(),
                    system_prompt.clone(),
                    session.history.clone(),
                    0, 128_000,
                    thinking_budget,
                    self.clients.clone(),
                );
                
                match engine.evaluate_userdata("on_before_inference", ctx.clone()) {
                    Ok(verdict) => {
                         if verdict.is_rejected() {
                             warn!(reason = %verdict.reason().unwrap_or(""), "Turn rejected by harness");
                             return Ok(false);
                         }
                    }
                    Err(e) => {
                         warn!(error = %e, "Harness on_before_inference error");
                    }
                }

                let state = ctx.get_state();
                session.history = state.messages;
                system_prompt = state.system_prompt;
                model = state.model;
                provider_name = state.provider;
                thinking_budget = state.thinking_budget;
            }
        }

        if !self.clients.contains_key(&provider_name) {
             if let Some(config) = self.config.providers.get(&provider_name) {
                 debug!(provider = %provider_name, "Lazily initializing provider");
                 match self.create_client(&provider_name, config) {
                     Ok(client) => { self.clients.insert(provider_name.clone(), client); },
                     Err(e) => {
                         error!(provider = %provider_name, error = %e, "Failed to initialize provider");
                         anyhow::bail!("Failed to initialize provider '{}': {}", provider_name, e);
                     }
                 }
             } else {
                 anyhow::bail!("Provider '{}' not found in configuration", provider_name);
             }
        }

        let client = self.clients.get(&provider_name)
            .ok_or_else(|| anyhow::anyhow!("Provider '{}' not initialized", provider_name))?
            .clone();

        let tools = self.tool_registry.tool_definitions();

        let options = provider::InferenceOptions {
            max_tokens: None,
            temperature: None,
            thinking_budget: Some(thinking_budget),
        };

        let mut stream = client.stream(&model, &system_prompt, &session.history, &tools, &options).await?;
        
        let mut response_text = String::with_capacity(4096);
        let mut pending_tool_calls: Vec<PendingToolCall> = Vec::new();

        while let Some(event_result) = stream.next().await {
             let event = event_result?;
             match &event {
                KernelEvent::MessageDelta { content_delta } => {
                    if !self.json {
                        print!("{}", content_delta);
                        io::stdout().flush().ok();
                    }
                    self.persist_event(session, &event);
                    response_text.push_str(content_delta);
                }
                KernelEvent::ThinkingDelta { thinking: _ } => {
                    // We don't append thinking to response_text
                    self.persist_event(session, &event);
                }
                KernelEvent::MessageEnd { input_tokens, output_tokens, .. } => {
                    session.total_input_tokens += *input_tokens as u64;
                    session.total_output_tokens += *output_tokens as u64;
                    self.persist_event(session, &event);
                }
                KernelEvent::ToolCall { id, name, args } => {
                    self.persist_event(session, &event);
                    pending_tool_calls.push(PendingToolCall {
                        id: id.clone(), name: name.clone(), args: args.clone()
                    });
                }
                _ => { self.persist_event(session, &event); }
             }
        }

        if !response_text.is_empty() && !response_text.ends_with('\n') { println!(); }

        let has_tool_calls = !pending_tool_calls.is_empty();

        self.persist_event(session, &KernelEvent::TurnEnd {
            turn_index: session.turn_index,
            has_tool_calls,
        });

         if let Some(ref store) = self.state {
            let content: Vec<serde_json::Value> = {
                let mut parts = Vec::new();
                if !response_text.is_empty() {
                    parts.push(serde_json::json!({"type": "text", "text": response_text}));
                }
                for tc in &pending_tool_calls {
                    parts.push(serde_json::json!({
                        "type": "tool_use", "id": tc.id, "name": tc.name, "input": tc.args,
                    }));
                }
                parts
            };
            let _ = store.insert_message(&session_id, session.turn_index, "assistant", &serde_json::Value::Array(content), None).await;
        }

        let mut assistant_content: Vec<InferenceContent> = Vec::new();
        if !response_text.is_empty() {
            assistant_content.push(InferenceContent::Text { text: response_text.clone() });
        }
        for tc in &pending_tool_calls {
            assistant_content.push(InferenceContent::ToolUse {
                id: tc.id.clone(), name: tc.name.clone(), input: tc.args.clone(),
            });
        }
        session.history.push(InferenceMessage {
            role: InferenceRole::Assistant,
            content: assistant_content,
            tool_call_id: None,
        });

        if !has_tool_calls {
            return Ok(false);
        }

        // Execute tools
        // Phase 1: Evaluate verdicts
        let mut validated_calls = Vec::new();
        let mut tool_results: Vec<InferenceContent> = Vec::new();

        for tc in &pending_tool_calls {
            let verdict = self.evaluate_tool_call(&tc.name, &tc.id, &tc.args).await;
            match &verdict {
                Verdict::Reject(reason) => {
                     warn!(tool = %tc.name, reason = %reason, "Tool REJECTED by harness");
                     self.persist_event(session, &KernelEvent::ToolExecStart { id: tc.id.clone(), name: tc.name.clone() });
                     self.persist_event(session, &KernelEvent::ToolExecEnd { id: tc.id.clone(), success: false });
                     let msg = format!("[HARNESS REJECTED] Tool '{}' blocked: {}", tc.name, reason);
                     
                     if let Some(ref store) = self.state {
                          let _ = store.insert_tool_execution(&session_id, session.turn_index, &tc.id, &tc.name, &tc.args, Some(&msg), true, Some(0), &verdict.to_string()).await;
                     }
                     tool_results.push(InferenceContent::ToolResult { tool_use_id: tc.id.clone(), content: msg, is_error: true });
                }
                Verdict::Escalate(reason) => {
                     warn!(tool = %tc.name, reason = %reason, "ESCALATION: Tool requires approval");
                     eprint!("[bedrock] Allow? (y/n): ");
                     io::stderr().flush().ok();
                     let mut input = String::new();
                     let approved = io::stdin().lock().read_line(&mut input).is_ok() && input.trim().eq_ignore_ascii_case("y");
                     if !approved {
                          warn!(tool = %tc.name, "Tool DENIED by user");
                          let msg = format!("[ESCALATION DENIED] Tool '{}' denied: {}", tc.name, reason);
                           self.persist_event(session, &KernelEvent::ToolExecStart { id: tc.id.clone(), name: tc.name.clone() });
                           self.persist_event(session, &KernelEvent::ToolExecEnd { id: tc.id.clone(), success: false });
                           if let Some(ref store) = self.state {
                                let _ = store.insert_tool_execution(&session_id, session.turn_index, &tc.id, &tc.name, &tc.args, Some(&msg), true, Some(0), "escalate_denied").await;
                           }
                           tool_results.push(InferenceContent::ToolResult { tool_use_id: tc.id.clone(), content: msg, is_error: true });
                     } else {
                         info!(tool = %tc.name, "Tool APPROVED by user");
                         validated_calls.push((tc, verdict));
                     }
                }
                Verdict::Allow | Verdict::Modify(_) => {
                    validated_calls.push((tc, verdict));
                }
            }
        }

        // Phase 2: Parallel Execution
        let kernel = &*self;
        let event_tx = session.event_tx.clone();
        let turn_index = session.turn_index;
        let futures = validated_calls.into_iter().map(|(tc, verdict)| {
            let session_id = session_id.clone();
            let tool_ctx = tool_ctx.clone();
            let event_tx = event_tx.clone();
            async move {
                let verdict_str = verdict.to_string();
                let final_args = match verdict {
                    Verdict::Modify(new_args) => {
                         info!(tool = %tc.name, "Tool arguments MODIFIED by harness");
                         new_args
                    },
                    _ => tc.args.clone()
                };

                let _ = event_tx.send((session_id.clone(), KernelEvent::ToolExecStart { id: tc.id.clone(), name: tc.name.clone() }));
                let start = Instant::now();
                let (content, is_error, metadata) = match kernel.tool_registry.execute(&tc.name, final_args, &tool_ctx).await {
                    Ok(o) => (o.content, false, o.metadata),
                    Err(e) => (format!("Tool error: {}", e), true, serde_json::Value::Null),
                };
                let duration_ms = start.elapsed().as_millis() as u64;
                
                let _ = event_tx.send((session_id.clone(), KernelEvent::ToolExecEnd { id: tc.id.clone(), success: !is_error }));

                if let Some(ref store) = kernel.state {
                     let _ = store.insert_tool_execution(&session_id, turn_index, &tc.id, &tc.name, &tc.args, Some(&content), is_error, Some(duration_ms), &verdict_str).await;
                }
                (tc, content, is_error, metadata)
            }
        });

        let execution_results = join_all(futures).await;

        // Phase 3: Side Effects & Result Collection
        for (tc, mut content, mut is_error, metadata) in execution_results {
            if !is_error {
                if let Some(action) = metadata.get("action").and_then(|v| v.as_str()) {
                    if action == "submit_task" {
                         let verdict_result = {
                            let harness = self.harness.lock().await;
                            if let Some(engine) = &*harness {
                                Some(engine.evaluate("on_task_submit", metadata.clone()))
                            } else { None }
                        };
                        
                        if let Some(result) = verdict_result {
                            match result {
                                Ok(Verdict::Allow) => {
                                    if let Some(subtasks) = metadata.get("subtasks").and_then(|v| v.as_array()) {
                                        if let Some(clear) = metadata.get("clear_existing").and_then(|v| v.as_bool()) {
                                            if clear { session.queue.lock().await.clear(); }
                                        }
                                        let mut q = session.queue.lock().await;
                                        for task in subtasks {
                                            if let Some(t) = task.as_str() { q.push_back(t.to_string()); }
                                        }
                                        debug!("tasks queued from submit_task");
                                    }
                                },
                                Ok(Verdict::Modify(new_tasks_val)) => {
                                     if let Some(new_tasks) = new_tasks_val.as_array() {
                                          let mut q = session.queue.lock().await;
                                          if let Some(clear) = metadata.get("clear_existing").and_then(|v| v.as_bool()) {
                                              if clear { q.clear(); }
                                          }
                                          for task in new_tasks {
                                              if let Some(t) = task.as_str() { q.push_back(t.to_string()); }
                                          }
                                           debug!("tasks queued (MODIFIED by harness)");
                                      }
                                },
                                Ok(Verdict::Reject(reason)) => {
                                    content = format!("Plan REJECTED by Harness: {}", reason);
                                },
                                Ok(Verdict::Escalate(reason)) => {
                                     content = format!("Plan paused for approval: {}", reason);
                                },
                                Err(e) => { error!(error = %e, "Failed to evaluate on_task_submit"); }
                            }
                        } else {
                            if let Some(subtasks) = metadata.get("subtasks").and_then(|v| v.as_array()) {
                                 let mut q = session.queue.lock().await;
                                 if let Some(clear) = metadata.get("clear_existing").and_then(|v| v.as_bool()) {
                                    if clear { q.clear(); }
                                 }
                                 for task in subtasks {
                                    if let Some(t) = task.as_str() { q.push_back(t.to_string()); }
                                 }
                            }
                        }
                    } else if action == "spawn_mcp" {
                          if let Some(cmd) = metadata.get("command").and_then(|v| v.as_str()) {
                               let args: Vec<String> = metadata.get("args")
                                  .and_then(|v| v.as_array())
                                  .map(|arr| arr.iter().map(|v| v.as_str().unwrap_or_default().to_string()).collect())
                                  .unwrap_or_default();
                               
                               match self.spawn_mcp_server(cmd, &args).await {
                                   Ok(count) => {
                                       content = format!("Successfully connected to MCP server. Loaded {} new tools.", count);
                                   },
                                   Err(e) => {
                                       content = format!("Failed to connect to MCP server: {}", e);
                                       is_error = true;
                                   }
                               }
                          }
                    }
                }
            }
            tool_results.push(InferenceContent::ToolResult { tool_use_id: tc.id.clone(), content, is_error });
        }

        session.history.push(InferenceMessage {
            role: InferenceRole::User,
            content: tool_results.clone(),
            tool_call_id: None,
        });

         if let Some(ref store) = self.state {
             let result_content: Vec<serde_json::Value> = tool_results.iter().map(|r| match r {
                 InferenceContent::ToolResult { tool_use_id, content, is_error } => {
                     serde_json::json!({ "type": "tool_result", "tool_use_id": tool_use_id, "content": content, "is_error": is_error })
                 }
                 _ => serde_json::json!({})
             }).collect();
             let _ = store.insert_message(&session_id, session.turn_index, "tool_result", &serde_json::Value::Array(result_content), None).await;
         }

         Ok(true)
    }

    /// Create the appropriate provider client from config.
    fn create_client(&self, _name: &str, config: &crate::kernel::config::ProviderConfig) -> Result<ProviderClient> {
        match config.kind.as_str() {
            "anthropic" => {
                let client = provider::create_anthropic_client(config)?;
                Ok(ProviderClient::new(
                    ProviderKind::Anthropic, // We might want to store the "kind" locally or make ProviderKind arbitrary? 
                    // ProviderKind is an enum. We should probably keep it for now as "Type" info.
                    client,
                ))
            }
            "openai" => {
                let client = provider::create_openai_client(config)?;
                Ok(ProviderClient::new(
                    ProviderKind::OpenAI,
                    client,
                ))
            }
            "mock" => {
                let client = provider::create_mock_client(config);
                 Ok(ProviderClient::new(
                    ProviderKind::Mock,
                    client,
                ))
            }
            _ => anyhow::bail!("Unknown provider type: {}", config.kind),
        }
    }

    /// Evaluate harness `on_tool_call` hook.
    ///
    /// Returns the composed verdict. If no harness is loaded, returns `Allow`.

    async fn evaluate_tool_call(&self, name: &str, id: &str, args: &serde_json::Value) -> Verdict {
        let harness = self.harness.lock().await;
        if let Some(ref engine) = *harness {
            let payload = serde_json::json!({
                "name": name,
                "id": id,
                "args": args,
            });
            match engine.evaluate("on_tool_call", payload) {
                Ok(verdict) => {
                    if !verdict.is_allowed() {
                        info!(tool = %name, verdict = %verdict, "Harness verdict");
                    }
                    verdict
                }
                Err(e) => {
                    // Harness evaluation errors are non-fatal — default to ALLOW
                    warn!(error = %e, "Harness on_tool_call error");
                    Verdict::Allow
                }
            }
        } else {
            Verdict::Allow
        }
    }

    /// Evaluate harness `on_token_usage` hook.
    ///
    /// This fires after each turn. If a harness rejects, it logs a warning.
    /// Budget enforcement via token hooks is informational in v0.1 — a REJECT here
    /// logs but doesn't halt the loop (the harness can use `db.kv_set` to track state
    /// and reject tool calls instead).

    async fn evaluate_token_usage(&self, input_tokens: u64, output_tokens: u64) {
        let harness = self.harness.lock().await;
        if let Some(ref engine) = *harness {
            let payload = serde_json::json!({
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": input_tokens + output_tokens,
            });
            match engine.evaluate("on_token_usage", payload) {
                Ok(verdict) => {
                    if verdict.is_rejected() {
                        warn!(reason = %verdict.reason().unwrap_or("budget exceeded"), "Token usage harness rejection");
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Harness on_token_usage error");
                }
            }
        }
    }

    /// Persist an event to the state store in the background.
    #[instrument(skip(self, session, event), fields(event_type = %event.event_type()))]
    pub fn persist_event(&self, session: &SessionState, event: &KernelEvent) {
        self.persist_event_internal(&session.event_tx, &session.id, event);
    }

    /// Internal helper for persistence (used by parallel runners)
    fn persist_event_internal(&self, tx: &mpsc::UnboundedSender<(String, KernelEvent)>, session_id: &str, event: &KernelEvent) {
        if self.json {
            // In JSON mode, all events go to stdout as NDJSON
            println!("{}", serde_json::to_string(event).unwrap_or_default());
        }
        if let Err(e) = tx.send((session_id.to_string(), event.clone())) {
            warn!(error = %e, "Failed to send event to background persistence task");
        }
    }

    /// Connect to an MCP server, initialize it, and register its tools.
    #[instrument(skip(self, args), fields(command = %command, args = ?args))]
    async fn spawn_mcp_server(&mut self, command: &str, args: &[String]) -> Result<usize> {
        let args_str: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        info!("Connecting to MCP server");

        let transport = StdioTransport::new(command, &args_str)
            .with_context(|| format!("Failed to spawn MCP process: {}", command))?;
        
        let client = McpClient::new(transport);
        client.initialize().await.with_context(|| "Failed to initialize MCP client")?;
        
        let list_result = client.list_tools().await.with_context(|| "Failed to list MCP tools")?;
        let count = list_result.tools.len();
        
        let client_arc = Arc::new(client);
        self.mcp_clients.push(client_arc.clone());

        for tool_def in list_result.tools {
            let proxy = McpToolProxy::new(client_arc.clone(), tool_def);
            self.tool_registry.register(Box::new(proxy))
                .with_context(|| "Failed to register MCP tool")?;
        }

        info!(count = count, "MCP tools registered");

        Ok(count)
    }
}
