//! Bedrock-SL globals injected into the Luau harness VM.
//!
//! These provide all capabilities that harness scripts have access to.
//! The harness VM itself is sandboxed — these are the only OS-touching APIs.

use mlua::{Lua, LuaSerdeExt, Result as LuaResult, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use glob::glob;

use crate::persistence::state::StateStore;
use crate::inference::provider::{
    ProviderClient
};
use crate::inference::embeddings::EmbeddingProvider;
use std::collections::{HashMap, VecDeque};

/// Shared state passed to async Lua callbacks via app data.
pub struct HarnessAppData {
    pub fs_root: PathBuf,
    pub workspace_root: PathBuf,
    pub state_store: Option<Arc<Mutex<StateStore>>>,
    pub clients: HashMap<String, ProviderClient>,
    pub embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    pub queue: Arc<Mutex<VecDeque<String>>>,
    pub config: Arc<crate::kernel::config::BedrockConfig>,
}

/// Register all Bedrock-SL globals into the Lua VM.
pub fn register_globals(lua: &Lua, app_data: HarnessAppData) -> LuaResult<()> {
    register_verdict_constants(lua)?;
    register_fs_module(lua, &app_data)?;
    register_db_module(lua, &app_data)?;
    register_json_module(lua)?;
    register_time_module(lua)?;
    register_session_module(lua, &app_data)?;
    register_bedrock_module(lua, &app_data)?;
    register_agent_module(lua, &app_data)?;
    register_log_function(lua)?;

    // Store app data for later access
    lua.set_app_data(app_data);

    Ok(())
}

/// Register ALLOW, REJECT, ESCALATE as integer constants.
///
/// Lua convention:
///   return ALLOW           -- proceed
///   return REJECT, "reason" -- block
///   return ESCALATE, "reason" -- ask human
fn register_verdict_constants(lua: &Lua) -> LuaResult<()> {
    let globals = lua.globals();
    globals.set("ALLOW", 1)?;
    globals.set("REJECT", 2)?;
    globals.set("ESCALATE", 3)?;
    globals.set("MODIFY", 4)?;
    Ok(())
}

/// Resolve a path relative to a root, ensuring it stays within the root.
/// Returns None if the path escapes the root.
fn resolve_safe_path(root: &Path, path_str: &str) -> Option<PathBuf> {
    let path = Path::new(path_str);
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };

    // Canonicalize both, checking that resolved starts with root
    // For non-existent paths, check the parent
    let canonical_root = root.canonicalize().ok()?;

    if let Ok(canonical) = resolved.canonicalize() {
        if canonical.starts_with(&canonical_root) {
            return Some(canonical);
        }
    } else {
        // File doesn't exist yet — check parent directory
        if let Some(parent) = resolved.parent() {
            if let Ok(canonical_parent) = parent.canonicalize() {
                if canonical_parent.starts_with(&canonical_root) {
                    return Some(canonical_parent.join(resolved.file_name()?));
                }
            }
        }
    }
    None
}

/// Register `fs` table: read, write, exists, list, is_safe_path
fn register_fs_module(lua: &Lua, app_data: &HarnessAppData) -> LuaResult<()> {
    let fs_table = lua.create_table()?;
    let fs_root = app_data.fs_root.clone();

    // fs.read(path) -> string | nil
    {
        let root = fs_root.clone();
        fs_table.set("read", lua.create_function(move |_lua, path: String| {
            match resolve_safe_path(&root, &path) {
                Some(safe_path) => match std::fs::read_to_string(&safe_path) {
                    Ok(content) => Ok(Value::String(_lua.create_string(&content)?)),
                    Err(_) => Ok(Value::Nil),
                },
                None => Ok(Value::Nil),
            }
        })?)?;
    }

    // fs.write(path, content) -> boolean
    {
        let root = fs_root.clone();
        fs_table.set("write", lua.create_function(move |_lua, (path, content): (String, String)| {
            match resolve_safe_path(&root, &path) {
                Some(safe_path) => {
                    // Create parent directories if needed
                    if let Some(parent) = safe_path.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    match std::fs::write(&safe_path, &content) {
                        Ok(_) => Ok(true),
                        Err(_) => Ok(false),
                    }
                },
                None => Ok(false),
            }
        })?)?;
    }

    // fs.exists(path) -> boolean
    {
        let root = fs_root.clone();
        fs_table.set("exists", lua.create_function(move |_lua, path: String| {
            match resolve_safe_path(&root, &path) {
                Some(safe_path) => Ok(safe_path.exists()),
                None => Ok(false),
            }
        })?)?;
    }

    // fs.list(path) -> table | nil
    {
        let root = fs_root.clone();
        fs_table.set("list", lua.create_function(move |lua, path: String| {
            match resolve_safe_path(&root, &path) {
                Some(safe_path) => {
                    match std::fs::read_dir(&safe_path) {
                        Ok(entries) => {
                            let tbl = lua.create_table()?;
                            let mut i = 1;
                            for entry in entries.flatten() {
                                tbl.set(i, entry.file_name().to_string_lossy().to_string())?;
                                i += 1;
                            }
                            Ok(Value::Table(tbl))
                        }
                        Err(_) => Ok(Value::Nil),
                    }
                }
                None => Ok(Value::Nil),
            }
        })?)?;
    }

    // fs.is_safe_path(path) -> boolean
    {
        let root = fs_root.clone();
        fs_table.set("is_safe_path", lua.create_function(move |_lua, path: String| {
            Ok(resolve_safe_path(&root, &path).is_some())
        })?)?;
    }

    lua.globals().set("fs", fs_table)?;
    Ok(())
}

/// Register `db` table: kv_get, kv_set
///
/// Note: These are synchronous wrappers around the async StateStore.
/// They use `tokio::task::block_in_place` to bridge sync Lua callbacks
/// to async Rust. This is acceptable because harness hook evaluation
/// is a brief synchronous step in the otherwise async agent loop.
fn register_db_module(lua: &Lua, app_data: &HarnessAppData) -> LuaResult<()> {
    let db_table = lua.create_table()?;

    // db.kv_get(key) -> string | nil
    {
        let store = app_data.state_store.clone();
        db_table.set("kv_get", lua.create_function(move |lua, key: String| {
            match &store {
                Some(store) => {
                    let store = store.clone();
                    let result = tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async {
                            let store = store.lock().await;
                            store.kv_get(&key).await
                        })
                    });
                    match result {
                        Ok(Some(val)) => Ok(Value::String(lua.create_string(&val)?)),
                        _ => Ok(Value::Nil),
                    }
                }
                None => Ok(Value::Nil),
            }
        })?)?;
    }

    // db.kv_set(key, value) -> boolean
    {
        let store = app_data.state_store.clone();
        db_table.set("kv_set", lua.create_function(move |_lua, (key, value): (String, String)| {
            match &store {
                Some(store) => {
                    let store = store.clone();
                    let result = tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async {
                            let store = store.lock().await;
                            store.kv_set(&key, &value).await
                        })
                    });
                    Ok(result.is_ok())
                }
                None => Ok(false),
            }
        })?)?;
    }

    lua.globals().set("db", db_table)?;
    Ok(())
}

/// Register `json` table: encode, decode
fn register_json_module(lua: &Lua) -> LuaResult<()> {
    let json_table = lua.create_table()?;

    // json.encode(table) -> string
    json_table.set("encode", lua.create_function(|lua, value: Value| {
        let json_val: serde_json::Value = lua.from_value(value)?;
        let s = serde_json::to_string(&json_val)
            .map_err(|e| mlua::Error::runtime(format!("JSON encode error: {}", e)))?;
        Ok(s)
    })?)?;

    // json.decode(string) -> table
    json_table.set("decode", lua.create_function(|lua, s: String| {
        let json_val: serde_json::Value = serde_json::from_str(&s)
            .map_err(|e| mlua::Error::runtime(format!("JSON decode error: {}", e)))?;
        let lua_val = lua.to_value(&json_val)?;
        Ok(lua_val)
    })?)?;

    lua.globals().set("json", json_table)?;
    Ok(())
}

/// Register `time` table: now_utc
fn register_time_module(lua: &Lua) -> LuaResult<()> {
    let time_table = lua.create_table()?;

    // time.now_utc() -> string (ISO 8601)
    time_table.set("now_utc", lua.create_function(|_lua, ()| {
        use std::time::SystemTime;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Format as ISO 8601 (basic — no chrono dependency)
        Ok(format!("{}", now))
    })?)?;

    lua.globals().set("time", time_table)?;
    Ok(())
}

/// Register `session` table: list, load
fn register_session_module(lua: &Lua, app_data: &HarnessAppData) -> LuaResult<()> {
    let session_table = lua.create_table()?;

    // session.list(limit?, offset?) -> { "id1", "id2", ... }
    {
        let store = app_data.state_store.clone();
        session_table.set("list", lua.create_function(move |_lua, (limit, offset): (Option<usize>, Option<usize>)| {
            let limit = limit.unwrap_or(10);
            let offset = offset.unwrap_or(0);
            match &store {
                Some(store) => {
                    let store = store.clone();
                    let result = tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async {
                            let store = store.lock().await;
                            store.list_sessions(limit, offset).await
                        })
                    });
                    match result {
                        Ok(sessions) => Ok(sessions),
                        Err(e) => Err(mlua::Error::runtime(format!("Failed to list sessions: {}", e))),
                    }
                }
                None => Ok(Vec::new()),
            }
        })?)?;
    }

    // session.load(id) -> { {role=..., content=...}, ... }
    {
        let store = app_data.state_store.clone();
        session_table.set("load", lua.create_function(move |lua, id: String| {
             match &store {
                Some(store) => {
                    let store = store.clone();
                    let result = tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async {
                            let store = store.lock().await;
                            store.get_messages(&id).await
                        })
                    });
                    
                    match result {
                        Ok(rows) => {
                            let tbl = lua.create_table()?;
                            for (i, row) in rows.into_iter().enumerate() {
                                let msg_tbl = lua.create_table()?;
                                msg_tbl.set("role", row.role)?;
                                // content is JSON string. Decode it.
                                let content_json: serde_json::Value = serde_json::from_str(&row.content)
                                    .map_err(|e| mlua::Error::runtime(format!("Failed to parse message content: {}", e)))?;
                                msg_tbl.set("content", lua.to_value(&content_json)?)?;
                                tbl.set(i + 1, msg_tbl)?;
                            }
                            Ok(Value::Table(tbl))
                        }
                         Err(e) => Err(mlua::Error::runtime(format!("Failed to load session '{}': {}", id, e))),
                    }
                }
                None => Ok(Value::Nil),
             }
        })?)?;
    }

    // session.queue(command) -> void
    {
        let queue = app_data.queue.clone();
        session_table.set("queue", lua.create_function(move |_lua, command: String| {
            let queue = queue.clone();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let mut q = queue.lock().await;
                    q.push_back(command);
                })
            });
            Ok(())
        })?)?;
    }
    
    // session.queue_all(commands) -> void
    {
        let queue = app_data.queue.clone();
        session_table.set("queue_all", lua.create_function(move |_lua, commands: Vec<String>| {
            let queue = queue.clone();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let mut q = queue.lock().await;
                    for cmd in commands {
                         q.push_back(cmd);
                    }
                })
            });
            Ok(())
        })?)?;
    }

    // session.queue_next(command) -> void
    {
        let queue = app_data.queue.clone();
        session_table.set("queue_next", lua.create_function(move |_lua, command: String| {
            let queue = queue.clone();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                     let mut q = queue.lock().await;
                     q.push_front(command);
                })
            });
            Ok(())
        })?)?;
    }

    lua.globals().set("session", session_table)?;
    Ok(())
}

/// Register `bedrock` table: context
fn register_bedrock_module(lua: &Lua, app_data: &HarnessAppData) -> LuaResult<()> {
    let bedrock_table = lua.create_table()?;
    let context_table = lua.create_table()?;
    let fs_root = app_data.fs_root.clone();

    // bedrock.context.glob(pattern) -> { "file1", "file2" }
    {
        let root = fs_root.clone();
        context_table.set("glob", lua.create_function(move |_lua, pattern: String| {
            let mut matches = Vec::new();
            // Construct absolute pattern
            let full_pattern = root.join(&pattern);
            let full_pattern_str = full_pattern.to_string_lossy();
            
            // Check for obvious traversal attempts in pattern itself before globbing
            if pattern.contains("..") {
                 // Simple safety check, though glob crate handles some traversals
                 return Ok(Vec::new());
            }

            if let Ok(paths) = glob(&full_pattern_str) {
                for entry in paths {
                    if let Ok(path) = entry {
                        // Ensure path is within root
                        if let Ok(canonical) = path.canonicalize() {
                             if let Ok(canonical_root) = root.canonicalize() {
                                 if canonical.starts_with(&canonical_root) {
                                     // Return relative path string
                                     if let Ok(relative) = path.strip_prefix(&root) {
                                         matches.push(relative.to_string_lossy().to_string());
                                     }
                                 }
                             }
                        }
                    }
                }
            }
            Ok(matches)
        })?)?;
    }

    bedrock_table.set("context", context_table)?;

    // bedrock.complete(prompt, options) -> string | nil
    {
        let clients = app_data.clients.clone();
        let config_arc = app_data.config.clone();
        
        bedrock_table.set("complete", lua.create_function(move |_lua, (prompt, options): (String, Option<mlua::Table>)| {
            let mut model = config_arc.agent.model.clone();
            let mut provider = config_arc.agent.provider.clone(); 
            // Default provider from config.
            
            // Check options for overrides
            if let Some(opts) = options {
                if let Ok(m) = opts.get("model") {
                    model = m;
                }
                if let Ok(p) = opts.get("provider") {
                    provider = p;
                }
            }

            // Lookup client by name directly
            let client = clients.get(&provider)
                 .ok_or_else(|| mlua::Error::RuntimeError(format!("Provider '{}' not initialized", provider)))?
                 .clone();

            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    client.completion(&model, "You are a helpful assistant.", &[
                        crate::inference::provider::InferenceMessage {
                            role: crate::inference::provider::InferenceRole::User,
                            content: vec![crate::inference::provider::InferenceContent::Text { text: prompt }],
                        }
                    ]).await
                })
            });

            match result {
                Ok(content) => Ok(Some(content)),
                Err(e) => Err(mlua::Error::RuntimeError(format!("Completion failed: {}", e))),
            }
        })?)?;
    }
    
    // bedrock.memory sub-module
    {
        let memory_table = lua.create_table()?;
        let store = app_data.state_store.clone();
        let embedding_provider = app_data.embedding_provider.clone();

        // bedrock.memory.store(content, metadata) -> boolean
        // This is a heavy operation (embedding + db insert), so we block carefully.
        {
             let store = store.clone();
             let embedding_provider = embedding_provider.clone();
             memory_table.set("store", lua.create_function(move |lua, (content, metadata): (String, Option<Value>)| {
                 let store = store.clone();
                 let embedding_provider = embedding_provider.clone();
                 let metadata_json: serde_json::Value = if let Some(meta) = metadata {
                     lua.from_value(meta)?
                 } else {
                     serde_json::json!({})
                 };

                 let result = tokio::task::block_in_place(|| {
                     tokio::runtime::Handle::current().block_on(async {
                         // 1. Generate embedding
                         if let Some(provider) = &embedding_provider {
                             let embedding = provider.embed(&content).await
                                 .map_err(|e| format!("Embedding failed: {}", e))?;
                             
                             // 2. Insert into DB
                             if let Some(store) = &store {
                                 let store = store.lock().await;
                                 store.insert_memory("current_session", &content, &embedding.vector, &metadata_json).await
                                     .map_err(|e| format!("DB insert failed: {}", e))?;
                                 Ok(true)
                             } else {
                                 Err("No state store available".to_string())
                             }
                         } else {
                             Err("No embedding provider available".to_string())
                         }
                     })
                 });

                 match result {
                     Ok(_) => Ok(true),
                     Err(e) => Err(mlua::Error::runtime(e)),
                 }
             })?)?;
        }

        // bedrock.memory.search(query, limit) -> { {content=..., score=...}, ... }
        {
             let store = store.clone();
             let embedding_provider = embedding_provider.clone();
             memory_table.set("search", lua.create_function(move |lua, (query, limit): (String, Option<usize>)| {
                 let store = store.clone();
                 let embedding_provider = embedding_provider.clone();
                 let limit = limit.unwrap_or(5);

                 let result = tokio::task::block_in_place(|| {
                     tokio::runtime::Handle::current().block_on(async {
                         // 1. Generate embedding for query
                         if let Some(provider) = &embedding_provider {
                             let embedding = provider.embed(&query).await
                                 .map_err(|e| format!("Embedding failed: {}", e))?;
                             
                             // 2. Search DB
                             if let Some(store) = &store {
                                 let store = store.lock().await;
                                 let results = store.search_memories("current_session", &embedding.vector, limit).await
                                     .map_err(|e| format!("DB search failed: {}", e))?;
                                 Ok(results)
                             } else {
                                 Err("No state store available".to_string())
                             }
                         } else {
                             Err("No embedding provider available".to_string())
                         }
                     })
                 });

                 match result {
                     Ok(rows) => {
                         let tbl = lua.create_table()?;
                         for (i, row) in rows.into_iter().enumerate() {
                             let row_tbl = lua.create_table()?;
                             row_tbl.set("content", row.content)?;
                             row_tbl.set("score", row.score)?;
                             // Parse metadata if needed, for now just raw string or ignore
                             // row_tbl.set("metadata", ...)?; 
                             tbl.set(i + 1, row_tbl)?;
                         }
                         Ok(Value::Table(tbl))
                     },
                     Err(e) => Err(mlua::Error::runtime(e)),
                 }
             })?)?;
        }

        bedrock_table.set("memory", memory_table)?;
    }

    lua.globals().set("bedrock", bedrock_table)?;
    Ok(())
}

/// Register `bedrock.agent` table: spawn
fn register_agent_module(lua: &Lua, app_data: &HarnessAppData) -> LuaResult<()> {
    let agent_table = lua.create_table()?;
    let bedrock_table: mlua::Table = lua.globals().get("bedrock")?; // Get existing table or fail? 
    // Wait, register_bedrock_module creates "bedrock" global. We should probably attach to it?
    // But `register_bedrock_module` sets "bedrock" global at the end.
    // Order matters. `register_bedrock_module` is called before this.
    // So we can get "bedrock" global and add "agent" to it.
    
    // bedrock.agent.spawn(prompt, options) -> string | nil
    {
        let config_arc = app_data.config.clone();
        let clients = app_data.clients.clone();
        let state_store = app_data.state_store.clone();
        
        agent_table.set("spawn", lua.create_function(move |_lua, (prompt, options): (String, Option<mlua::Table>)| {
            let mut config = (*config_arc).clone();
            
            // Apply options
            if let Some(opts) = options {
                if let Ok(m) = opts.get("model") {
                    config.agent.model = m;
                }
                if let Ok(sp) = opts.get("system_prompt") {
                    config.agent.system_prompt = sp;
                }
                if let Ok(mt) = opts.get("max_turns") {
                    config.kernel.max_turns = mt;
                }
                if let Ok(p) = opts.get("provider") {
                    config.agent.provider = p;
                }
            }

            let clients = clients.clone();
            let state_store = state_store.clone();

            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    // Create sub-kernel
                    let mut kernel = crate::kernel::Kernel::new(config, false, false); // Not verbose/json for subtask
                    
                    // Inject shared components
                    kernel.clients = clients;
                    if let Some(s) = state_store {
                         let guard = s.lock().await;
                         kernel.state = Some(guard.clone());
                    }
                    
                    // Init harness for sub-kernel
                    if let Err(e) = kernel.init_harness() {
                        return Err(format!("Sub-kernel harness init failed: {}", e));
                    }
                    
                    // Run
                    if let Err(e) = kernel.run(Some(prompt)).await {
                        return Err(format!("Sub-kernel run failed: {}", e));
                    }
                    
                    // Extract result
                    // Get last message from assistant
                    if let Some(last) = kernel.history.last() {
                         if last.role == crate::inference::provider::InferenceRole::Assistant {
                             // Join text content
                             let text = last.content.iter().filter_map(|c| match c {
                                 crate::inference::provider::InferenceContent::Text { text } => Some(text.as_str()),
                                 _ => None,
                             }).collect::<Vec<_>>().join("\n");
                             Ok(text)
                         } else {
                             Ok("".to_string())
                         }
                    } else {
                        Ok("".to_string())
                    }
                })
            });

            match result {
                Ok(s) => Ok(Some(s)),
                Err(e) => Err(mlua::Error::runtime(e)),
            }
        })?)?;
    }
    
    bedrock_table.set("agent", agent_table)?;
    Ok(())
}

/// Register `log(msg)` global function
fn register_log_function(lua: &Lua) -> LuaResult<()> {
    lua.globals().set("log", lua.create_function(|_lua, msg: String| {
        eprintln!("[harness] {}", msg);
        Ok(())
    })?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_app_data(dir: &Path) -> HarnessAppData {
        HarnessAppData {
            fs_root: dir.to_path_buf(),
            workspace_root: dir.to_path_buf(),
            state_store: None,
            client: None,
            embedding_provider: None,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            config: Arc::new(crate::kernel::config::BedrockConfig {
                agent: crate::kernel::config::AgentConfig {
                    system_prompt: "test".to_string(),
                    model: "test".to_string(),
                    provider: "openai".to_string(),
                    thinking: None,
                },
                kernel: crate::kernel::config::KernelConfig::default(),
                persistence: crate::kernel::config::PersistenceConfig::default(),
                harness: crate::kernel::config::HarnessConfig::default(),
                providers: crate::kernel::config::ProvidersConfig::default(),
                embeddings: None,
            }),
        }
    }

    #[test]
    fn test_verdict_constants() {
        let lua = Lua::new();
        register_verdict_constants(&lua).unwrap();
        let globals = lua.globals();
        assert_eq!(globals.get::<i32>("ALLOW").unwrap(), 1);
        assert_eq!(globals.get::<i32>("REJECT").unwrap(), 2);
        assert_eq!(globals.get::<i32>("ESCALATE").unwrap(), 3);
    }

    #[test]
    fn test_fs_read_and_exists() {
        let dir = TempDir::new().unwrap();
        let test_file = dir.path().join("hello.txt");
        std::fs::write(&test_file, "hello world").unwrap();

        let lua = Lua::new();
        let app_data = create_test_app_data(dir.path());
        register_globals(&lua, app_data).unwrap();

        // Read existing file
        let result: String = lua.load("return fs.read('hello.txt')").eval().unwrap();
        assert_eq!(result, "hello world");

        // Exists
        let exists: bool = lua.load("return fs.exists('hello.txt')").eval().unwrap();
        assert!(exists);

        // Non-existent
        let missing: Value = lua.load("return fs.read('nope.txt')").eval().unwrap();
        assert_eq!(missing, Value::Nil);
    }

    #[test]
    fn test_fs_write() {
        let dir = TempDir::new().unwrap();
        let lua = Lua::new();
        let app_data = create_test_app_data(dir.path());
        register_globals(&lua, app_data).unwrap();

        // Write a file
        let ok: bool = lua.load("return fs.write('output.txt', 'written by lua')").eval().unwrap();
        assert!(ok);

        let content = std::fs::read_to_string(dir.path().join("output.txt")).unwrap();
        assert_eq!(content, "written by lua");
    }

    #[test]
    fn test_fs_path_traversal_blocked() {
        let dir = TempDir::new().unwrap();
        let lua = Lua::new();
        let app_data = create_test_app_data(dir.path());
        register_globals(&lua, app_data).unwrap();

        // Path traversal should return nil/false
        let result: Value = lua.load("return fs.read('../../../etc/passwd')").eval().unwrap();
        assert_eq!(result, Value::Nil);

        let safe: bool = lua.load("return fs.is_safe_path('../../../etc/passwd')").eval().unwrap();
        assert!(!safe);
    }

    #[test]
    fn test_fs_list() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.txt"), "").unwrap();
        std::fs::write(dir.path().join("b.txt"), "").unwrap();

        let lua = Lua::new();
        let app_data = create_test_app_data(dir.path());
        register_globals(&lua, app_data).unwrap();

        let count: i32 = lua.load("return #fs.list('.')").eval().unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_json_encode_decode() {
        let lua = Lua::new();
        let app_data = HarnessAppData {
            fs_root: PathBuf::from("."),
            workspace_root: PathBuf::from("."),
            state_store: None,
            client: None,
            embedding_provider: None,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            config: Arc::new(crate::kernel::config::BedrockConfig {
                agent: crate::kernel::config::AgentConfig {
                    system_prompt: "test".to_string(),
                    model: "test".to_string(),
                    provider: "openai".to_string(),
                    thinking: None,
                },
                kernel: crate::kernel::config::KernelConfig::default(),
                persistence: crate::kernel::config::PersistenceConfig::default(),
                harness: crate::kernel::config::HarnessConfig::default(),
                providers: crate::kernel::config::ProvidersConfig::default(),
                embeddings: None,
            }),
        };
        register_globals(&lua, app_data).unwrap();

        let result: String = lua.load(r#"return json.encode({name = "bedrock", version = 1})"#).eval().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["name"], "bedrock");

        let decoded: String = lua.load(r#"
            local t = json.decode('{"key": "value"}')
            return t.key
        "#).eval().unwrap();
        assert_eq!(decoded, "value");
    }

    #[test]
    fn test_log_function() {
        let lua = Lua::new();
        let app_data = HarnessAppData {
            fs_root: PathBuf::from("."),
            workspace_root: PathBuf::from("."),
            state_store: None,
            client: None,
            embedding_provider: None,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            config: Arc::new(crate::kernel::config::BedrockConfig {
                agent: crate::kernel::config::AgentConfig {
                    system_prompt: "test".to_string(),
                    model: "test".to_string(),
                    provider: "openai".to_string(),
                    thinking: None,
                },
                kernel: crate::kernel::config::KernelConfig::default(),
                persistence: crate::kernel::config::PersistenceConfig::default(),
                harness: crate::kernel::config::HarnessConfig::default(),
                providers: crate::kernel::config::ProvidersConfig::default(),
                embeddings: None,
            }),
        };
        register_globals(&lua, app_data).unwrap();

        // Just verify it doesn't panic
        lua.load("log('test message from harness')").exec().unwrap();
    }

    #[test]
    fn test_time_now_utc() {
        let lua = Lua::new();
        let app_data = HarnessAppData {
            fs_root: PathBuf::from("."),
            workspace_root: PathBuf::from("."),
            state_store: None,
            client: None,
            embedding_provider: None,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            config: Arc::new(crate::kernel::config::BedrockConfig {
                agent: crate::kernel::config::AgentConfig {
                    system_prompt: "test".to_string(),
                    model: "test".to_string(),
                    provider: "openai".to_string(),
                    thinking: None,
                },
                kernel: crate::kernel::config::KernelConfig::default(),
                persistence: crate::kernel::config::PersistenceConfig::default(),
                harness: crate::kernel::config::HarnessConfig::default(),
                providers: crate::kernel::config::ProvidersConfig::default(),
                embeddings: None,
            }),
        };
        register_globals(&lua, app_data).unwrap();

        let timestamp: String = lua.load("return time.now_utc()").eval().unwrap();
        // Should be a numeric string (Unix timestamp)
        let ts: u64 = timestamp.parse().unwrap();
        assert!(ts > 1_700_000_000); // After 2023
    }
}
