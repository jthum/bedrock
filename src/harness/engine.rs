//! Harness Engine — loads and evaluates Luau harness scripts.
//!
//! The engine manages a sandboxed Luau VM, loads `.lua` files from a directory,
//! and evaluates hook functions against incoming events. Results are composed
//! using first-REJECT-wins semantics.

use anyhow::{Context, Result};
use mlua::{Function, Lua, LuaSerdeExt, MultiValue, Table, Value};
use std::path::Path;

use crate::harness::globals::{self, HarnessAppData};
use crate::harness::verdict::{Verdict, compose_verdicts};

/// The harness engine manages script loading and hook evaluation.
pub struct HarnessEngine {
    lua: Lua,
    /// Names of loaded scripts (in evaluation order)
    scripts: Vec<String>,
}

impl HarnessEngine {
    /// Create a new harness engine with sandboxed Luau VM.
    ///
    /// `app_data` provides the globals context (fs root, state store, etc.).
    pub fn new(app_data: HarnessAppData) -> Result<Self> {
        let lua = Lua::new();

        // Register all Bedrock-SL globals before sandboxing.
        // This makes them available but read-only once sandbox is enabled.
        globals::register_globals(&lua, app_data)
            .map_err(|e| anyhow::anyhow!("Failed to register harness globals: {}", e))?;

        // Enable Luau sandboxing:
        // - All libraries and built-in metatables become read-only
        // - Globals become read-only
        // - Removes access to dangerous functions (os, io, loadfile, etc.)
        lua.sandbox(true)
            .map_err(|e| anyhow::anyhow!("Failed to enable Luau sandbox: {}", e))?;

        Ok(Self {
            lua,
            scripts: Vec::new(),
        })
    }

    /// Load all `.lua` files from the given directory.
    ///
    /// Scripts are loaded in alphabetical order. Each script's hook functions
    /// are registered in the Lua environment. If the directory doesn't exist,
    /// no scripts are loaded (harness-free operation).
    pub fn load_dir(&mut self, dir: &Path) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }

        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .with_context(|| format!("Failed to read harness directory: {}", dir.display()))?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "lua")
                    .unwrap_or(false)
            })
            .collect();

        // Sort alphabetically for deterministic evaluation order
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let path = entry.path();
            let name = path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            let source = std::fs::read_to_string(&path)
                .with_context(|| format!("Failed to read harness script: {}", path.display()))?;

            self.load_script(&name, &source, &path)?;
        }

        Ok(())
    }

    /// Load a single harness script by name.
    ///
    /// Each script's hook functions are captured into a per-module table
    /// (`__harness_modules[name]`). In sandboxed Luau, `function NAME(...)`
    /// writes to the chunk's local environment, so we reference hooks directly
    /// by name after executing the script source.
    fn load_script(&mut self, name: &str, source: &str, path: &Path) -> Result<()> {
        let globals = self.lua.globals();

        // Ensure __harness_modules exists in global registry if not already
        if !globals.contains_key("__harness_modules")? {
            globals.set("__harness_modules", self.lua.create_table()?)?;
        }
        let modules: Table = globals.get("__harness_modules")?;

        // Create a sandboxed environment for this script.
        // Writes go to 'env', reads fall back to 'globals' (via __index).
        let env = self.lua.create_table()?;
        let meta = self.lua.create_table()?;
        meta.set("__index", globals)?;
        let _ = env.set_metatable(Some(meta));

        // Load and execute string in the sandboxed environment, capturing return value
        let retval: Value = self.lua.load(source)
            .set_name(format!("@{}", path.display()))
            .set_environment(env.clone())
            .eval()
            .map_err(|e| anyhow::anyhow!("Failed to load harness script '{}': {}", path.display(), e))?;

        // Extract known hooks: priority to return value (module table), fallback to env (globals)
        let module_exports = match retval {
            Value::Table(t) => t,
            _ => self.lua.create_table()?,
        };

        let known_hooks = [
            "on_tool_call",
            "on_token_usage",
            "on_agent_start",
            "on_agent_end",
            "on_before_inference",
            "on_task_submit",
        ];

        for hook in known_hooks {
            // If hook is already in exports (from return table), keep it.
            // Otherwise, check if it exists in the script's global env.
            if !module_exports.contains_key(hook)? {
                if let Ok(func) = env.get::<Function>(hook) {
                    module_exports.set(hook, func)?;
                }
            }
        }

        // Register the module
        modules.set(name, module_exports)?;
        self.scripts.push(name.to_string());
        Ok(())
    }

    /// Call a hook function across all loaded scripts and compose the verdicts.
    ///
    /// The hook receives a Lua table with the event payload. Each script's
    /// implementation of the hook (if any) is called in load order.
    /// Verdicts are composed using first-REJECT-wins semantics.
    pub fn evaluate(&self, hook_name: &str, payload: serde_json::Value) -> Result<Verdict> {
        let verdicts = self.call_hook(hook_name, payload)?;
        Ok(compose_verdicts(&verdicts))
    }

    /// Load and execute a Lua script string (for testing/verification).
    pub fn load_script_str(&mut self, script: &str) -> Result<()> {
        self.lua
            .load(script)
            .exec()
            .map_err(|e| anyhow::anyhow!("Script execution failed: {}", e))?;
        Ok(())
    }

    /// Call a hook with a UserData argument (e.g. ContextWrapper).
    pub fn evaluate_userdata(&self, hook_name: &str, data: impl mlua::UserData + Clone + Send + 'static) -> Result<Verdict> {
        let verdicts = self.call_hook_userdata(hook_name, data)?;
        Ok(compose_verdicts(&verdicts))
    }

    /// Call a hook across all loaded scripts, returning individual verdicts.
    fn call_hook(&self, hook_name: &str, payload: serde_json::Value) -> Result<Vec<Verdict>> {
        let mut verdicts = Vec::new();

        let modules: Value = self.lua.globals().get("__harness_modules")
            .unwrap_or(Value::Nil);

        let modules_table = match modules {
            Value::Table(t) => t,
            _ => return Ok(verdicts),
        };

        // Convert payload to Lua value
        let lua_payload = self.lua.to_value(&payload)
            .map_err(|e| anyhow::anyhow!("Failed to convert payload to Lua: {}", e))?;

        for script_name in &self.scripts {
            let module: Value = modules_table.get(script_name.as_str())
                .unwrap_or(Value::Nil);

            let module_table = match module {
                Value::Table(t) => t,
                _ => continue,
            };

            let hook_fn: Value = module_table.get(hook_name)
                .unwrap_or(Value::Nil);

            match hook_fn {
                Value::Function(func) => {
                    let result = func.call::<MultiValue>(lua_payload.clone())
                        .map_err(|e| anyhow::anyhow!(
                            "Harness '{}' hook '{}' failed: {}",
                            script_name, hook_name, e
                        ))?;

                    let verdict = parse_verdict(&self.lua, result)?;
                    verdicts.push(verdict);
                }
                _ => {
                    // Hook not defined in this script — skip (implicit ALLOW)
                    continue;
                }
            }
        }

        Ok(verdicts)
    }

    /// Call a hook with UserData, returning individual verdicts.
    fn call_hook_userdata(&self, hook_name: &str, data: impl mlua::UserData + Clone + Send + 'static) -> Result<Vec<Verdict>> {
        let mut verdicts = Vec::new();

        let modules: Value = self.lua.globals().get("__harness_modules")
            .unwrap_or(Value::Nil);

        let modules_table = match modules {
            Value::Table(t) => t,
            _ => return Ok(verdicts),
        };

        for name in &self.scripts {
             if let Ok(module) = modules_table.get::<Table>(name.as_str()) {
                if let Ok(func) = module.get::<Function>(hook_name) {
                    let ud = self.lua.create_userdata(data.clone()).map_err(|e| {
                         anyhow::anyhow!("Failed to create userdata for hook '{}': {}", hook_name, e)
                    })?;

                    match func.call::<Value>(ud) {
                        Ok(result) => {
                            if let Ok(v) = serde_json::from_value::<Verdict>(
                                self.lua.from_value(result.clone()).unwrap_or(serde_json::Value::Null)
                            ) {
                                verdicts.push(v);
                            }
                        }
                        Err(e) => {
                            eprintln!("[bedrock] Error in hook '{}' (script '{}'): {}", hook_name, name, e);
                        }
                    }
                }
             }
        }

        Ok(verdicts)
    }

    /// Get the names of loaded scripts.
    pub fn loaded_scripts(&self) -> &[String] {
        &self.scripts
    }
}

/// Parse a Lua return value into a Verdict.
///
/// Convention:
///   return ALLOW              → Verdict::Allow
///   return REJECT, "reason"   → Verdict::Reject(reason)
///   return ESCALATE, "reason" → Verdict::Escalate(reason)
///   return MODIFY, {new_data} → Verdict::Modify(json_data)
fn parse_verdict(lua: &Lua, values: MultiValue) -> Result<Verdict> {
    let mut iter = values.into_iter();

    let verdict_code = match iter.next() {
        Some(Value::Integer(n)) => n,
        Some(Value::Nil) | None => return Ok(Verdict::Allow), // No return = ALLOW
        other => {
            return Err(anyhow::anyhow!(
                "Harness hook returned non-integer verdict: {:?}",
                other
            ));
        }
    };

    match verdict_code {
        1 => Ok(Verdict::Allow),
        2 | 3 => {
            let reason = match iter.next() {
                Some(Value::String(s)) => s.to_str()
                    .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in verdict reason: {}", e))?
                    .to_string(),
                _ => String::new(),
            };
            if verdict_code == 2 {
                Ok(Verdict::Reject(reason))
            } else {
                Ok(Verdict::Escalate(reason))
            }
        },
        4 => {
             let val = match iter.next() {
                Some(v) => lua.from_value::<serde_json::Value>(v)
                    .map_err(|e| anyhow::anyhow!("Failed to convert MODIFY value to JSON: {}", e))?,
                None => serde_json::Value::Null,
             };
             Ok(Verdict::Modify(val))
        },
        _ => Err(anyhow::anyhow!("Unknown verdict code: {}", verdict_code)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::path::PathBuf;

    fn test_app_data() -> HarnessAppData {
        HarnessAppData {
            fs_root: PathBuf::from("."),
            workspace_root: PathBuf::from("."),
            state_store: None,
            clients: std::collections::HashMap::new(),
            embedding_provider: None,
            queue: std::sync::Arc::new(tokio::sync::Mutex::new(Some(std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new()))))),
            config: std::sync::Arc::new(crate::kernel::config::BedrockConfig::default()),
        }
    }

    #[test]
    fn test_engine_no_scripts() {
        let engine = HarnessEngine::new(test_app_data()).unwrap();
        assert!(engine.loaded_scripts().is_empty());

        let verdict = engine.evaluate("on_tool_call", serde_json::json!({})).unwrap();
        assert_eq!(verdict, Verdict::Allow);
    }

    #[test]
    fn test_engine_load_empty_dir() {
        let dir = TempDir::new().unwrap();
        let mut engine = HarnessEngine::new(test_app_data()).unwrap();
        engine.load_dir(dir.path()).unwrap();
        assert!(engine.loaded_scripts().is_empty());
    }

    #[test]
    fn test_engine_load_nonexistent_dir() {
        let mut engine = HarnessEngine::new(test_app_data()).unwrap();
        engine.load_dir(Path::new("/nonexistent/path")).unwrap();
        assert!(engine.loaded_scripts().is_empty());
    }

    #[test]
    fn test_engine_allow_verdict() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("allow.lua"),
            r#"
            function on_tool_call(call)
                return ALLOW
            end
            "#,
        ).unwrap();

        let mut engine = HarnessEngine::new(test_app_data()).unwrap();
        engine.load_dir(dir.path()).unwrap();

        let verdict = engine
            .evaluate("on_tool_call", serde_json::json!({"name": "read_file", "args": {}}))
            .unwrap();
        assert_eq!(verdict, Verdict::Allow);
    }

    #[test]
    fn test_engine_reject_verdict() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("safety.lua"),
            r#"
            function on_tool_call(call)
                if call.name == "shell_exec" then
                    return REJECT, "Shell commands are not allowed"
                end
                return ALLOW
            end
            "#,
        ).unwrap();

        let mut engine = HarnessEngine::new(test_app_data()).unwrap();
        engine.load_dir(dir.path()).unwrap();

        // shell_exec should be rejected
        let verdict = engine
            .evaluate(
                "on_tool_call",
                serde_json::json!({"name": "shell_exec", "args": {"command": "ls"}}),
            )
            .unwrap();
        assert!(verdict.is_rejected());
        assert_eq!(verdict.reason(), Some("Shell commands are not allowed"));

        // read_file should be allowed
        let verdict = engine
            .evaluate(
                "on_tool_call",
                serde_json::json!({"name": "read_file", "args": {"path": "foo.txt"}}),
            )
            .unwrap();
        assert_eq!(verdict, Verdict::Allow);
    }

    #[test]
    fn test_engine_escalate_verdict() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("escalation.lua"),
            r#"
            function on_tool_call(call)
                if call.name == "write_file" then
                    return ESCALATE, "File writes need human approval"
                end
                return ALLOW
            end
            "#,
        ).unwrap();

        let mut engine = HarnessEngine::new(test_app_data()).unwrap();
        engine.load_dir(dir.path()).unwrap();

        let verdict = engine
            .evaluate(
                "on_tool_call",
                serde_json::json!({"name": "write_file", "args": {}}),
            )
            .unwrap();
        assert!(verdict.is_escalated());
        assert_eq!(verdict.reason(), Some("File writes need human approval"));
    }

    #[test]
    fn test_engine_composition_reject_wins() {
        let dir = TempDir::new().unwrap();
        // Script "a" allows everything
        std::fs::write(
            dir.path().join("a_permissive.lua"),
            r#"
            function on_tool_call(call)
                return ALLOW
            end
            "#,
        ).unwrap();

        // Script "b" rejects shell_exec
        std::fs::write(
            dir.path().join("b_safety.lua"),
            r#"
            function on_tool_call(call)
                if call.name == "shell_exec" then
                    return REJECT, "Blocked by safety harness"
                end
                return ALLOW
            end
            "#,
        ).unwrap();

        let mut engine = HarnessEngine::new(test_app_data()).unwrap();
        engine.load_dir(dir.path()).unwrap();

        assert_eq!(engine.loaded_scripts(), &["a_permissive", "b_safety"]);

        let verdict = engine
            .evaluate(
                "on_tool_call",
                serde_json::json!({"name": "shell_exec", "args": {}}),
            )
            .unwrap();
        assert_eq!(
            verdict,
            Verdict::Reject("Blocked by safety harness".to_string())
        );
    }

    #[test]
    fn test_engine_rm_rf_blocked() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("safety.lua"),
            r#"
            function on_tool_call(call)
                if call.name == "shell_exec" then
                    local cmd = call.args.command
                    if cmd and cmd:find("rm %-rf") then
                        return REJECT, "Destructive command 'rm -rf' is not allowed"
                    end
                end
                return ALLOW
            end
            "#,
        ).unwrap();

        let mut engine = HarnessEngine::new(test_app_data()).unwrap();
        engine.load_dir(dir.path()).unwrap();

        // rm -rf should be blocked
        let verdict = engine
            .evaluate(
                "on_tool_call",
                serde_json::json!({"name": "shell_exec", "args": {"command": "rm -rf /"}}),
            )
            .unwrap();
        assert_eq!(
            verdict,
            Verdict::Reject("Destructive command 'rm -rf' is not allowed".to_string())
        );

        // Safe commands should pass
        let verdict = engine
            .evaluate(
                "on_tool_call",
                serde_json::json!({"name": "shell_exec", "args": {"command": "ls -la"}}),
            )
            .unwrap();
        assert_eq!(verdict, Verdict::Allow);
    }

    #[test]
    fn test_engine_undefined_hook_returns_allow() {
        let dir = TempDir::new().unwrap();
        // Script only defines on_tool_call, not on_token_usage
        std::fs::write(
            dir.path().join("partial.lua"),
            r#"
            function on_tool_call(call)
                return ALLOW
            end
            "#,
        ).unwrap();

        let mut engine = HarnessEngine::new(test_app_data()).unwrap();
        engine.load_dir(dir.path()).unwrap();

        // Calling undefined hook should return ALLOW (no opinions)
        let verdict = engine
            .evaluate("on_token_usage", serde_json::json!({}))
            .unwrap();
        assert_eq!(verdict, Verdict::Allow);
    }

    #[test]
    fn test_engine_token_usage_hook() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("budget.lua"),
            r#"
            function on_token_usage(usage)
                if usage.total_cost_usd and usage.total_cost_usd > 1.0 then
                    return REJECT, "Budget exceeded: $" .. tostring(usage.total_cost_usd)
                end
                return ALLOW
            end
            "#,
        ).unwrap();

        let mut engine = HarnessEngine::new(test_app_data()).unwrap();
        engine.load_dir(dir.path()).unwrap();

        // Under budget
        let verdict = engine
            .evaluate(
                "on_token_usage",
                serde_json::json!({"total_cost_usd": 0.5, "input_tokens": 100, "output_tokens": 50}),
            )
            .unwrap();
        assert_eq!(verdict, Verdict::Allow);

        // Over budget
        let verdict = engine
            .evaluate(
                "on_token_usage",
                serde_json::json!({"total_cost_usd": 1.5, "input_tokens": 100, "output_tokens": 50}),
            )
            .unwrap();
        assert!(verdict.is_rejected());
    }

    #[test]
    fn test_engine_modify_verdict() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("modify.lua"),
            r#"
            function on_task_submit(payload)
                return MODIFY, { "Modified Task 1", "Modified Task 2" }
            end
            "#,
        ).unwrap();

        let mut engine = HarnessEngine::new(test_app_data()).unwrap();
        engine.load_dir(dir.path()).unwrap();

        let verdict = engine
            .evaluate(
                "on_task_submit",
                serde_json::json!({"action": "submit_task"}),
            )
            .unwrap();

        match verdict {
             Verdict::Modify(val) => {
                 let arr = val.as_array().unwrap();
                 assert_eq!(arr.len(), 2);
                 assert_eq!(arr[0].as_str().unwrap(), "Modified Task 1");
                 assert_eq!(arr[1].as_str().unwrap(), "Modified Task 2");
             },
             _ => panic!("Expected Modify verdict, got {:?}", verdict),
        }
    }
}
