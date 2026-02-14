use std::sync::{Arc, Mutex};
use mlua::{UserData, UserDataMethods, MetaMethod, Value, LuaSerdeExt};

use crate::inference::provider::{InferenceMessage, ProviderClient};
use std::collections::HashMap;

/// Inner state shareable between Rust and Lua
#[derive(Clone, Debug)]
pub struct ContextState {
    pub model: String,
    pub provider: String,
    pub system_prompt: String,
    pub messages: Vec<InferenceMessage>,
    pub token_count: u32,
    pub token_limit: u32,
    pub thinking_budget: u32,
}

/// UserData wrapper for Context validation and mutation
#[derive(Clone)]
pub struct ContextWrapper {
    pub state: Arc<Mutex<ContextState>>,
    pub clients: HashMap<String, ProviderClient>,
}

impl ContextWrapper {
    pub fn new(
        model: String,
        provider: String,
        system_prompt: String,
        messages: Vec<InferenceMessage>,
        token_count: u32,
        token_limit: u32,
        thinking_budget: u32,
        clients: HashMap<String, ProviderClient>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ContextState {
                model,
                provider,
                system_prompt,
                messages,
                token_count,
                token_limit,
                thinking_budget,
            })),
            clients,
        }
    }

    /// Retrieve the inner state (cloning the data out)
    pub fn get_state(&self) -> ContextState {
        self.state.lock().unwrap().clone()
    }
}

impl UserData for ContextWrapper {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // Properties
        methods.add_method("get_model", |_, this, ()| {
            let state = this.state.lock().unwrap();
            Ok(state.model.clone())
        });
        
        methods.add_method("get_provider", |_, this, ()| {
            let state = this.state.lock().unwrap();
            Ok(state.provider.clone())
        });

        methods.add_method("get_token_count", |_, this, ()| {
            let state = this.state.lock().unwrap();
            Ok(state.token_count)
        });

        methods.add_method("get_token_limit", |_, this, ()| {
            let state = this.state.lock().unwrap();
            Ok(state.token_limit)
        });

        methods.add_method("get_system_prompt", |_, this, ()| {
            let state = this.state.lock().unwrap();
            Ok(state.system_prompt.clone())
        });

        methods.add_method("set_system_prompt", |_, this, val: String| {
            let mut state = this.state.lock().unwrap();
            state.system_prompt = val;
            Ok(())
        });

        methods.add_method("get_thinking_budget", |_, this, ()| {
            let state = this.state.lock().unwrap();
            Ok(state.thinking_budget)
        });

        methods.add_method("set_thinking_budget", |_, this, val: u32| {
            let mut state = this.state.lock().unwrap();
            state.thinking_budget = val;
            Ok(())
        });

        // Messages Property (Copy)
        methods.add_method("get_messages", |lua, this: &ContextWrapper, ()| {
            let state = this.state.lock().unwrap();
            let val = lua.to_value(&state.messages).map_err(mlua::Error::external)?;
            Ok(val)
        });

        methods.add_method("set_messages", |lua, this: &ContextWrapper, val: Value| {
            let messages: Vec<InferenceMessage> = lua.from_value(val).map_err(mlua::Error::external)?;
            let mut state = this.state.lock().unwrap();
            state.messages = messages;
            Ok(())
        });

        // Metatable __index/__newindex for properties
        methods.add_meta_method(MetaMethod::Index, |lua, this: &ContextWrapper, key: String| {
            match key.as_str() {
                "model" => {
                    let state = this.state.lock().unwrap();
                    Ok(Value::String(lua.create_string(&state.model)?))
                }
                "provider" => {
                    let state = this.state.lock().unwrap();
                    Ok(Value::String(lua.create_string(&state.provider)?))
                }
                "token_count" => {
                    let state = this.state.lock().unwrap();
                    Ok(Value::Integer(state.token_count as i64)) 
                }
                "token_limit" => {
                    let state = this.state.lock().unwrap();
                    Ok(Value::Integer(state.token_limit as i64))
                }
                "system_prompt" => {
                    let state = this.state.lock().unwrap();
                    Ok(Value::String(lua.create_string(&state.system_prompt)?))
                }
                "thinking_budget" => {
                    let state = this.state.lock().unwrap();
                    Ok(Value::Integer(state.thinking_budget as i64))
                }
                "messages" => {
                    let state = this.state.lock().unwrap();
                    lua.to_value(&state.messages).map_err(mlua::Error::external)
                }
                _ => Ok(Value::Nil),
            }
        });

        methods.add_meta_method(MetaMethod::NewIndex, |lua, this: &ContextWrapper, (key, val): (String, Value)| {
            match key.as_str() {
                "system_prompt" => {
                    let s: String = lua.from_value(val).map_err(mlua::Error::external)?;
                    let mut state = this.state.lock().unwrap();
                    state.system_prompt = s;
                    Ok(())
                }
                "provider" => {
                    let s: String = lua.from_value(val).map_err(mlua::Error::external)?;
                    let mut state = this.state.lock().unwrap();
                    state.provider = s;
                    Ok(())
                }
                "thinking_budget" => {
                    let b: u32 = lua.from_value(val).map_err(mlua::Error::external)?;
                    let mut state = this.state.lock().unwrap();
                    state.thinking_budget = b;
                    Ok(())
                }
                "messages" => {
                    let msgs: Vec<InferenceMessage> = lua.from_value(val).map_err(mlua::Error::external)?;
                    let mut state = this.state.lock().unwrap();
                    state.messages = msgs;
                    Ok(())
                }
                 _ => Err(mlua::Error::RuntimeError(format!("Cannot set read-only or unknown property: {}", key))),
            }
        });

        // Mutation Helpers
        methods.add_method("add_message", |lua, this: &ContextWrapper, val: Value| {
            let msg: InferenceMessage = lua.from_value(val).map_err(mlua::Error::external)?;
            let mut state = this.state.lock().unwrap();
            state.messages.push(msg);
            Ok(())
        });

        methods.add_method("remove_message", |_, this, idx: usize| {
            let mut state = this.state.lock().unwrap();
            // Lua is 1-indexed, Rust is 0-indexed
            if idx > 0 && idx <= state.messages.len() {
                state.messages.remove(idx - 1);
                Ok(())
            } else {
                Err(mlua::Error::RuntimeError(format!("Index out of bounds: {}", idx)))
            }
        });

        methods.add_method("clear_messages", |_, this, ()| {
            let mut state = this.state.lock().unwrap();
            state.messages.clear();
            Ok(())
        });

        // Summarize Capability
        // Takes a table of messages (optional) or uses current messages
        // Summarize Capability (Sync wrapper)
        // Summarize Capability (Sync wrapper)
        methods.add_method("summarize", |lua, this: &ContextWrapper, args: Value| {
             let clients = this.clients.clone();
             let state_arc = this.state.clone();
             
             let messages_opt: Option<Vec<InferenceMessage>> = if args.is_nil() {
                 None
             } else {
                 Some(lua.from_value(args).map_err(mlua::Error::external)?)
             };

             // Use block_in_place to bridge async client to sync Lua
             let res = tokio::task::block_in_place(|| {
                 tokio::runtime::Handle::current().block_on(async {
                     let (messages, model, provider_name) = {
                         let state = state_arc.lock().unwrap();
                         let msgs = messages_opt.unwrap_or_else(|| state.messages.clone());
                         (msgs, state.model.clone(), state.provider.clone())
                     };
                     
                     // Helper to map error
                     let get_client = || -> Result<ProviderClient, String> {
                         clients.get(&provider_name)
                             .cloned()
                             .ok_or_else(|| format!("Provider '{}' not initialized", provider_name))
                     };

                     match get_client() {
                         Ok(client) => {
                             let system_prompt = "Summarize the following conversation concisely.";
                             client.completion(&model, system_prompt, &messages).await
                                 .map_err(|e| format!("Completion failed: {}", e))
                         },
                         Err(e) => Err(e),
                     }
                 })
             });
             
             match res {
                 Ok(summary) => Ok(Some(summary)), // Return string
                 Err(e) => Err(mlua::Error::RuntimeError(format!("Summarization failed: {}", e))),
             }
        });
    }
}
