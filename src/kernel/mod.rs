pub mod config;
pub mod event;

use anyhow::{Context, Result};
use config::BedrockConfig;
use event::KernelEvent;
use futures::StreamExt;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
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
    pub config: BedrockConfig,
    pub verbose: bool,
    pub json: bool,
    pub tool_registry: ToolRegistry,
    pub state: Option<StateStore>,
    /// Thread-safe harness engine for hot-reloading
    pub harness: Arc<Mutex<Option<HarnessEngine>>>,
    /// Watcher handle to keep it alive
    pub check_watcher: Option<RecommendedWatcher>,
    pub clients: HashMap<String, ProviderClient>,
    pub embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    // Persistent session state
    pub queue: Arc<Mutex<VecDeque<String>>>,
    pub session_id: String,
    pub history: Vec<InferenceMessage>,
    pub turn_index: u32,
    pub total_input_tokens: u32,
    pub total_output_tokens: u32,
    /// Active MCP clients (kept alive for tool execution). 
    /// TODO: Implement a way to cleanup or limit these for long-running sessions.
    pub mcp_clients: Vec<Arc<McpClient<StdioTransport>>>,
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
    pub fn new(config: BedrockConfig, verbose: bool, json: bool) -> Self {
        let tool_registry = create_default_registry();
        Self {
            config,
            verbose,
            json,
            tool_registry,
            state: None,
            harness: Arc::new(Mutex::new(None)),
            check_watcher: None,
            clients: HashMap::new(),
            embedding_provider: None,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            session_id: uuid::Uuid::new_v4().to_string(),
            history: Vec::new(),
            turn_index: 0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            mcp_clients: Vec::new(),
        }
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
                        api_key: openai_config.api_key_env.as_ref().map(|k| std::env::var(k).unwrap_or_default()).unwrap_or_default(),
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
        if self.verbose {
            eprintln!("[bedrock] State store initialized: {}", db_path);
        }
        self.state = Some(store);
        Ok(())
    }

    /// Initialize the harness engine. Call after `init_state()` and before `run()`.
    pub async fn init_harness(&mut self) -> Result<()> {
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
            state_store: self.state.as_ref().map(|s| {
                // This is safe because harness evaluation is synchronous and brief.
                Arc::new(Mutex::new(s.clone()))
            }),
            clients: self.clients.clone(),
            embedding_provider: self.embedding_provider.clone(),
            queue: self.queue.clone(),
            config: Arc::new(self.config.clone()),
        };

        let mut engine = HarnessEngine::new(app_data)
            .with_context(|| "Failed to create harness engine")?;

        engine.load_dir(&harness_dir)
            .with_context(|| format!("Failed to load harness scripts from '{}'", harness_dir.display()))?;

        let script_count = engine.loaded_scripts().len();
        if self.verbose {
            if script_count > 0 {
                eprintln!(
                    "[bedrock] Harness loaded: {} script(s) from '{}'",
                    script_count,
                    harness_dir.display()
                );
                for name in engine.loaded_scripts() {
                    eprintln!("[bedrock]   - {}.lua", name);
                }
            } else {
                eprintln!("[bedrock] No harness scripts found in '{}'", harness_dir.display());
            }
        }

        {
            let mut h = self.harness.lock().await;
            *h = Some(engine);
        }
        Ok(())
    }

    /// Reload the harness from disk (atomic swap).
    pub async fn reload_harness(&self) -> Result<()> {
        Self::reload_harness_static(
            self.harness.clone(),
            self.config.clone(),
            self.clients.clone(),
            self.state.clone(),
            self.embedding_provider.clone(),
            self.queue.clone(),
            self.verbose,
        ).await
    }

    /// Static version of reload_harness that can be called from background tasks.
    pub async fn reload_harness_static(
        harness_lock: Arc<Mutex<Option<HarnessEngine>>>,
        config: BedrockConfig,
        clients: HashMap<String, ProviderClient>,
        state: Option<StateStore>,
        embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
        queue: Arc<Mutex<VecDeque<String>>>,
        verbose: bool
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
            state_store: state.as_ref().map(|s| {
                Arc::new(Mutex::new(s.clone()))
            }),
            clients: clients.clone(),
            embedding_provider: embedding_provider.clone(),
            queue: queue.clone(),
            config: Arc::new(config.clone()),
        };

        match HarnessEngine::new(app_data) {
            Ok(mut engine) => {
                 match engine.load_dir(&harness_dir) {
                     Ok(_) => {
                         let script_count = engine.loaded_scripts().len();
                         let mut h = harness_lock.lock().await;
                         *h = Some(engine);
                         if verbose {
                             eprintln!("[bedrock] Harness reloaded: {} scripts", script_count);
                         }
                         Ok(())
                     },
                     Err(e) => {
                         eprintln!("[bedrock] Failed to load harness scripts: {}", e);
                         Err(e)
                     }
                 }
            },
            Err(e) => {
                eprintln!("[bedrock] Failed to create harness engine: {}", e);
                Err(e)
            }
        }
    }

    /// Start watching the harness directory for changes (background thread).
    pub fn start_watcher(&mut self) -> Result<()> {
        use notify::{RecursiveMode, Watcher};
        use std::time::Duration;

        let harness_lock = self.harness.clone();
        let config = self.config.clone();
        let clients = self.clients.clone();
        let state = self.state.clone();
        let embedding_provider = self.embedding_provider.clone();
        let queue = self.queue.clone();
        let verbose = self.verbose;
        let harness_dir = PathBuf::from(&config.harness.directory);

        if !harness_dir.exists() {
            if verbose {
                eprintln!("[bedrock] Harness directory does not exist, skipping watcher: {}", harness_dir.display());
            }
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

                if verbose {
                    eprintln!("[bedrock] Hot-reload triggered by file change...");
                }

                let _ = Self::reload_harness_static(
                    harness_lock.clone(),
                    config.clone(),
                    clients.clone(),
                    state.clone(),
                    embedding_provider.clone(),
                    queue.clone(),
                    verbose,
                ).await;
            }
        });

        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            match res {
                Ok(event) => {
                    if event.kind.is_modify() || event.kind.is_create() || event.kind.is_remove() {
                        let _ = tx.blocking_send(());
                    }
                }
                Err(e) => eprintln!("[bedrock] Watcher error: {:?}", e),
            }
        })?;

        watcher.watch(&harness_dir, RecursiveMode::NonRecursive)?;
        self.check_watcher = Some(watcher);

        if verbose {
            eprintln!("[bedrock] Watching harness directory: {}", harness_dir.display());
        }

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
    /// The loop: send messages → stream response → collect tool calls →
    /// execute tools → append results → repeat until no tool calls or max turns.
    /// Run the agent by processing the command queue.
    ///
    /// If `initial_prompt` is provided, it is added to the queue first.
    /// The function returns when the queue is clear.
    pub async fn run(&mut self, initial_prompt: Option<String>) -> Result<()> {
         if let Some(prompt) = initial_prompt {
             self.queue_prompt(prompt).await;
         }
         
         // ─── Harness Hook: on_agent_start ────────────────────────────
         // Run only at the beginning of the session
             if self.turn_index == 0 {
                 self.persist_event(&self.session_id.clone(), &KernelEvent::AgentStart {
                    session_id: self.session_id.clone(),
                 }).await;
    
                 {
                    let harness = self.harness.lock().await;
                    if let Some(ref engine) = *harness {
                        use crate::harness::verdict::Verdict;
                        let verdict = engine.evaluate(
                            "on_agent_start",
                            serde_json::json!({ "session_id": self.session_id }),
                        )?;
                        if let Verdict::Reject(reason) = verdict {
                            eprintln!("[bedrock] Session rejected by harness: {}", reason);
                            return Ok(());
                        }
                    }
                 }
             }

         loop {
             // Pop next task
             let task = {
                 let mut q = self.queue.lock().await;
                 q.pop_front()
             };

             if let Some(prompt) = task {
                 if self.verbose {
                     eprintln!("[bedrock] Starting task: {}", prompt);
                 }
                 self.run_task(&prompt).await?;
             } else {
                  // ─── Harness Hook: on_task_complete ─────────────────────
                  // Triggered when the queue is explicitly empty.
                  let mut recheck = false;
                  
                  let verdict_result = {
                      let harness = self.harness.lock().await;
                      if let Some(ref engine) = *harness {
                          let payload = serde_json::json!({
                              "session_id": self.session_id,
                              "turn_count": self.turn_index,
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
                                          let mut q = self.queue.lock().await;
                                          for task in new_tasks {
                                              if let Some(t) = task.as_str() {
                                                  q.push_back(t.to_string());
                                              }
                                          }
                                          eprintln!("[bedrock] Validation failed or extended. {} new tasks queued.", new_tasks.len());
                                          recheck = true;
                                      }
                                 }
                           },
                           Ok(Verdict::Reject(reason)) => {
                               eprintln!("[bedrock] Session ended with REJECTION: {}", reason);
                               // We break, effectively ending the session.
                           },
                           Ok(_) => {
                               // Allow or Escalate (ignored at this stage) -> Done.
                           },
                           Err(e) => {
                               if self.verbose { eprintln!("[bedrock] Warning: harness on_task_complete error: {}", e); }
                           }
                      }
                 }
                 
                 if recheck {
                     continue;
                 }
                 break;
             }
         }
         
         self.persist_event(&self.session_id.clone(), &KernelEvent::AgentEnd {
            message_count: self.turn_index,
            total_input_tokens: self.total_input_tokens,
            total_output_tokens: self.total_output_tokens,
         }).await;

         Ok(())
    }

    /// Add a prompt to the end of the queue.
    pub async fn queue_prompt(&self, prompt: String) {
        let mut q = self.queue.lock().await;
        q.push_back(prompt);
    }
    
    /// Execute a single task (one specific prompt) within the persistent session.
    // TODO: Decompose this method into smaller, testable units (e.g. process_stream_event, handle_tool_calls)
    async fn run_task(&mut self, prompt: &str) -> Result<()> {
        let session_id = self.session_id.clone();

        // Default provider/client selection moved inside the loop for dynamic switching.

        // Append user message to history
        self.history.push(InferenceMessage {
            role: InferenceRole::User,
            content: vec![InferenceContent::Text { text: prompt.to_string() }],
        });

        // Initial configuration (can be overridden by harness per turn)
        let mut model = self.config.agent.model.clone();
        let mut provider_name = self.config.agent.provider.clone();
        let mut system_prompt = self.config.agent.system_prompt.clone();
        // Tools will be fetched inside the loop to support dynamic updates

        let tool_ctx = ToolContext {
            workspace_root: std::path::PathBuf::from(&self.config.kernel.workspace_root),
            session_id: session_id.clone(),
        };

        // Persist user message
        if let Some(ref store) = self.state {
            let _ = store.insert_message(
                &session_id,
                self.turn_index,
                "user",
                &serde_json::json!([{"type": "text", "text": prompt}]),
                None,
            ).await;
        }

        // Multi-turn loop for this specific task
        // We limit turns *per task* or *per session*? 
        // Currently max_turns is in config. Let's assume it's per task for safety, 
        // or check global. Let's enforce it per task to prevent infinite loops on one query.
        let mut task_turn_count = 0;
        let max_task_turns = self.config.kernel.max_turns;

        loop {
            if task_turn_count >= max_task_turns {
                eprintln!("[bedrock] Max turns ({}) reached for this task.", max_task_turns);
                break;
            }

            self.persist_event(&session_id, &KernelEvent::TurnStart { turn_index: self.turn_index }).await;

            // ─── Harness Hook: on_before_inference ───────────────────────
            let _thinking_budget = self.config.agent.thinking.as_ref()
                .and_then(|t| if t.enabled { t.budget_tokens } else { None })
                .unwrap_or(0);

            let mut thinking_budget = self.config.agent.thinking.as_ref()
                .and_then(|t| if t.enabled { t.budget_tokens } else { None })
                .unwrap_or(0);

            // Scope for harness lock
            {
                let harness = self.harness.lock().await;
                if let Some(ref engine) = *harness {
                // Use stored history + system prompt
                // Note: we might want to let harness see full history
                let ctx = ContextWrapper::new(
                    model.clone(),
                    provider_name.clone(),
                    system_prompt.clone(),
                    self.history.clone(),
                    0, 128_000,
                    thinking_budget,
                    self.clients.clone(),
                );
                
                match engine.evaluate_userdata("on_before_inference", ctx.clone()) {
                    Ok(verdict) => {
                         if verdict.is_rejected() {
                             eprintln!("[bedrock] 🛑 Turn rejected by harness: {}", verdict.reason().unwrap_or(""));
                             break;
                         }
                    }
                    Err(e) => {
                         if self.verbose { eprintln!("[bedrock] Warning: harness on_before_inference error: {}", e); }
                    }
                }

                let state = ctx.get_state();
                // Update run_task state from harness modifications
                self.history = state.messages;
                system_prompt = state.system_prompt;
                model = state.model;
                provider_name = state.provider;
                thinking_budget = state.thinking_budget;
            }
            } // End harness lock

            // Resolve client for this turn based on `provider_name` (potentially modified by harness)
            
            // Lazy initialization if not present?
            if !self.clients.contains_key(&provider_name) {
                 if let Some(config) = self.config.providers.get(&provider_name) {
                     if self.verbose { eprintln!("[bedrock] Lazily initializing provider: {}", provider_name); }
                     match self.create_client(&provider_name, config) {
                         Ok(client) => { self.clients.insert(provider_name.clone(), client); },
                         Err(e) => {
                             eprintln!("[bedrock] Failed to initialize provider '{}': {}", provider_name, e);
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
                max_tokens: None, // Use default or config
                temperature: None,
                thinking_budget: Some(thinking_budget),
            };

            // Stream from provider
            let mut stream = client.stream(&model, &system_prompt, &self.history, &tools, &options).await?;
            
            let mut response_text = String::new();
            let mut pending_tool_calls: Vec<PendingToolCall> = Vec::new();

            while let Some(event_result) = stream.next().await {
                 let event = event_result?;
                 match &event {
                    KernelEvent::MessageDelta { content_delta } => {
                        if self.json {
                             println!("{}", serde_json::to_string(&event).unwrap_or_default());
                        } else {
                            print!("{}", content_delta);
                            io::stdout().flush().ok();
                        }
                        response_text.push_str(content_delta);
                    }
                    KernelEvent::ThinkingDelta { thinking } => {
                        if self.verbose {
                             eprint!("{}", thinking); // Print thinking to stderr in verbose mode
                             io::stderr().flush().ok();
                        }
                        // We don't append thinking to response_text (it's separate)
                        self.persist_event(&session_id, &event).await;
                    }
                    KernelEvent::MessageEnd { input_tokens, output_tokens, .. } => {
                        self.total_input_tokens += input_tokens;
                        self.total_output_tokens += output_tokens;
                        self.persist_event(&session_id, &event).await;
                    }
                    KernelEvent::ToolCall { id, name, args } => {
                        self.persist_event(&session_id, &event).await;
                        pending_tool_calls.push(PendingToolCall {
                            id: id.clone(), name: name.clone(), args: args.clone()
                        });
                    }
                    _ => { self.persist_event(&session_id, &event).await; }
                 }
            }

            if !response_text.is_empty() && !response_text.ends_with('\n') { println!(); }

            let has_tool_calls = !pending_tool_calls.is_empty();

            self.persist_event(&session_id, &KernelEvent::TurnEnd {
                turn_index: self.turn_index,
                has_tool_calls,
            }).await;

            // Persist assistant message
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
                let _ = store.insert_message(&session_id, self.turn_index, "assistant", &serde_json::Value::Array(content), None).await;
            }

            // Add assistant response to history
            let mut assistant_content: Vec<InferenceContent> = Vec::new();
            if !response_text.is_empty() {
                assistant_content.push(InferenceContent::Text { text: response_text.clone() });
            }
            for tc in &pending_tool_calls {
                assistant_content.push(InferenceContent::ToolUse {
                    id: tc.id.clone(), name: tc.name.clone(), input: tc.args.clone(),
                });
            }
            self.history.push(InferenceMessage {
                role: InferenceRole::Assistant,
                content: assistant_content,
            });

            if !has_tool_calls {
                self.turn_index += 1;
                break;
            }

            // Execute tools
            let mut tool_results: Vec<InferenceContent> = Vec::new();
            for tc in &pending_tool_calls {
                if self.verbose { eprintln!("[bedrock] Executing tool: {} ({})", tc.name, tc.id); }
                let verdict = self.evaluate_tool_call(&tc.name, &tc.id, &tc.args).await;
                let verdict_str = verdict.to_string();
                let mut final_args = tc.args.clone();

                match &verdict {
                     Verdict::Reject(reason) => {
                         eprintln!("[bedrock] Tool {} REJECTED: {}", tc.name, reason);
                          self.persist_event(&session_id, &KernelEvent::ToolExecStart { id: tc.id.clone(), name: tc.name.clone() }).await;
                          self.persist_event(&session_id, &KernelEvent::ToolExecEnd { id: tc.id.clone(), success: false }).await;
                          let msg = format!("[HARNESS REJECTED] Tool '{}' blocked: {}", tc.name, reason);
                          
                           if let Some(ref store) = self.state {
                                let _ = store.insert_tool_execution(&session_id, self.turn_index, &tc.id, &tc.name, &tc.args, Some(&msg), true, Some(0), &verdict_str).await;
                           }
                           tool_results.push(InferenceContent::ToolResult { tool_use_id: tc.id.clone(), content: msg, is_error: true });
                           continue;
                     }
                     Verdict::Escalate(reason) => {
                         eprintln!("\n[bedrock] ⚠ ESCALATION: Tool '{}' requires approval. Reason: {}", tc.name, reason);
                         eprint!("[bedrock] Allow? (y/n): ");
                         io::stderr().flush().ok();
                         let mut input = String::new();
                         let approved = io::stdin().lock().read_line(&mut input).is_ok() && input.trim().eq_ignore_ascii_case("y");
                         // (Abbreviated logic: if approved, fall through. If denied, similar to reject)
                         if !approved {
                              eprintln!("[bedrock] Tool {} DENIED.", tc.name);
                              let msg = format!("[ESCALATION DENIED] Tool '{}' denied: {}", tc.name, reason);
                               self.persist_event(&session_id, &KernelEvent::ToolExecStart { id: tc.id.clone(), name: tc.name.clone() }).await;
                               self.persist_event(&session_id, &KernelEvent::ToolExecEnd { id: tc.id.clone(), success: false }).await;
                               if let Some(ref store) = self.state {
                                    let _ = store.insert_tool_execution(&session_id, self.turn_index, &tc.id, &tc.name, &tc.args, Some(&msg), true, Some(0), "escalate_denied").await;
                               }
                               tool_results.push(InferenceContent::ToolResult { tool_use_id: tc.id.clone(), content: msg, is_error: true });
                               continue;
                         }
                         eprintln!("[bedrock] Tool {} APPROVED.", tc.name);
                     }
                     Verdict::Allow => {}
                     Verdict::Modify(new_args) => {
                         eprintln!("[bedrock] Tool arguments MODIFIED by harness.");
                         final_args = new_args.clone();
                     }
                }
                
                self.persist_event(&session_id, &KernelEvent::ToolExecStart { id: tc.id.clone(), name: tc.name.clone() }).await;
                let start = Instant::now();
                let (mut content, mut is_error, metadata) = match self.tool_registry.execute(&tc.name, final_args, &tool_ctx).await {
                    Ok(o) => (o.content, false, o.metadata),
                    Err(e) => (format!("Tool error: {}", e), true, serde_json::Value::Null),
                };

                // Intercept "submit_task" action
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
                                        // Proceed with queuing
                                        if let Some(subtasks) = metadata.get("subtasks").and_then(|v| v.as_array()) {
                                            if let Some(clear) = metadata.get("clear_existing").and_then(|v| v.as_bool()) {
                                                if clear {
                                                    let mut q = self.queue.lock().await;
                                                    q.clear();
                                                }
                                            }
                                            
                                            let mut q = self.queue.lock().await;
                                            for task in subtasks {
                                                if let Some(t) = task.as_str() {
                                                    q.push_back(t.to_string());
                                                }
                                            }
                                            if self.verbose { eprintln!("[bedrock] tasks queued from submit_task"); }
                                        }
                                    },
                                    Ok(Verdict::Modify(new_tasks_val)) => {
                                         if let Some(new_tasks) = new_tasks_val.as_array() {
                                              let mut q = self.queue.lock().await;
                                              
                                              if let Some(clear) = metadata.get("clear_existing").and_then(|v| v.as_bool()) {
                                                  if clear { q.clear(); }
                                              }

                                              for task in new_tasks {
                                                  if let Some(t) = task.as_str() {
                                                      q.push_back(t.to_string());
                                                  }
                                              }
                                              if self.verbose { eprintln!("[bedrock] tasks queued (MODIFIED by harness)"); }
                                         } else {
                                             eprintln!("[bedrock] Verdict::Modify returned non-array value, ignoring.");
                                         }
                                    },
                                    Ok(Verdict::Reject(reason)) => {
                                        content = format!("Plan REJECTED by Harness: {}", reason);
                                    },
                                    Ok(Verdict::Escalate(reason)) => {
                                         content = format!("Plan paused for approval: {}", reason);
                                         // Logic for escalation could go here if needed
                                    },
                                    Err(e) => {
                                        eprintln!("[bedrock] Failed to evaluate on_task_submit: {}", e);
                                    }
                                }
                            } else {
                                // No harness, just queue it
                                if let Some(subtasks) = metadata.get("subtasks").and_then(|v| v.as_array()) {
                                     let mut q = self.queue.lock().await;
                                     if let Some(clear) = metadata.get("clear_existing").and_then(|v| v.as_bool()) {
                                        if clear { q.clear(); }
                                     }
                                     for task in subtasks {
                                        if let Some(t) = task.as_str() {
                                            q.push_back(t.to_string());
                                        }
                                     }
                                }
                            }
                        } else if action == "spawn_mcp" {
                        // Handle MCP connection request
                        if let Some(cmd) = metadata.get("command").and_then(|v| v.as_str()) {
                             let args: Vec<String> = metadata.get("args")
                                .and_then(|v| v.as_array())
                                .map(|arr| arr.iter().map(|v| v.as_str().unwrap_or_default().to_string()).collect())
                                .unwrap_or_default();
                             
                             match self.spawn_mcp_server(cmd, &args).await {
                                 Ok(count) => {
                                     content = format!("Successfully connected to MCP server. Loaded {} new tools.", count);
                                     // Re-fetch tools for next loop iteration
                                     // Note: `tools` var in this loop is stale. But `self.tool_registry` is updated.
                                     // We should break/restart loop or just know that next turn uses new tools?
                                     // The `run_task` loop re-reads `tools` per turn (see line ~354).
                                     // This ensures that any tools dynamically registered via MCP mid-session 
                                     // are available in the next LLM turn.
                                     // We MUST update `tools` variable here for the model to see them in next turn!
                                     
                                     // However, `tools` is `Vec<ToolDefinition>`. We need to re-fetch.
                                     // But `tools` variable is defined outside the loop.
                                     // We can't update `tools` easily because of scope. 
                                     // Ideally, `run_task` loop gets fresh tools each turn.
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
                let duration_ms = start.elapsed().as_millis() as u64;
                self.persist_event(&session_id, &KernelEvent::ToolExecEnd { id: tc.id.clone(), success: !is_error }).await;

                if let Some(ref store) = self.state {
                     let _ = store.insert_tool_execution(&session_id, self.turn_index, &tc.id, &tc.name, &tc.args, Some(&content), is_error, Some(duration_ms), &verdict_str).await;
                }
                tool_results.push(InferenceContent::ToolResult { tool_use_id: tc.id.clone(), content, is_error });
            }

            self.history.push(InferenceMessage {
                role: InferenceRole::User,
                content: tool_results.clone(),
            });

             if let Some(ref store) = self.state {
                 // Persist tool results
                 // (JSON conversion similar to before)
                 let result_content: Vec<serde_json::Value> = tool_results.iter().map(|r| match r {
                     InferenceContent::ToolResult { tool_use_id, content, is_error } => {
                         serde_json::json!({ "type": "tool_result", "tool_use_id": tool_use_id, "content": content, "is_error": is_error })
                     }
                     _ => serde_json::json!({})
                 }).collect();
                 let _ = store.insert_message(&session_id, self.turn_index, "tool_result", &serde_json::Value::Array(result_content), None).await;
             }



             self.evaluate_token_usage(self.total_input_tokens, self.total_output_tokens).await;
             self.turn_index += 1;
             task_turn_count += 1;
        }
        Ok(())
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
                    if self.verbose && !verdict.is_allowed() {
                        eprintln!("[bedrock] Harness verdict for {}: {}", name, verdict);
                    }
                    verdict
                }
                Err(e) => {
                    // Harness evaluation errors are non-fatal — default to ALLOW
                    eprintln!("[bedrock] Warning: harness on_tool_call error: {}", e);
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

    async fn evaluate_token_usage(&self, input_tokens: u32, output_tokens: u32) {
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
                        eprintln!(
                            "[bedrock] ⚠ Token usage harness: {}",
                            verdict.reason().unwrap_or("budget exceeded")
                        );
                    }
                }
                Err(e) => {
                    if self.verbose {
                        eprintln!("[bedrock] Warning: harness on_token_usage error: {}", e);
                    }
                }
            }
        }
    }

    /// Persist an event — emits to verbose stderr and writes to state store.
    /// If json mode is on, emits to stdout as JSON line.
    async fn persist_event(&self, session_id: &str, event: &KernelEvent) {
        if self.json {
            // In JSON mode, all events go to stdout as NDJSON
            println!("{}", serde_json::to_string(event).unwrap_or_default());
        } else if self.verbose {
            let event_type = event.event_type();
            match event {
                KernelEvent::MessageDelta { .. } => {}
                _ => {
                    eprintln!("[bedrock:{}] {:?}", event_type, event);
                }
            }
        }

        if let Some(ref store) = self.state {
            let event_type = event.event_type().to_string();
            let payload = serde_json::to_value(event).unwrap_or(serde_json::json!({}));
            // Fire and forget — don't fail the agent loop if persistence fails
            if let Err(e) = store.insert_event(session_id, &event_type, &payload).await {
                if self.verbose {
                    eprintln!("[bedrock] Warning: failed to persist event: {}", e);
                }
            }
        }
    }

    /// Connect to an MCP server, initialize it, and register its tools.
    async fn spawn_mcp_server(&mut self, command: &str, args: &[String]) -> Result<usize> {
        let args_str: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        if self.verbose {
            eprintln!("[bedrock] Connecting to MCP server: {} {:?}", command, args);
        }

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
            self.tool_registry.register(Box::new(proxy));
        }

        if self.verbose {
            eprintln!("[bedrock] Registered {} MCP tools.", count);
        }

        Ok(count)
    }
}
