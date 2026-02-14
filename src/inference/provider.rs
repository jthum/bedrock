use std::str::FromStr;
use anyhow::{Context, Result};
use futures::stream::{Stream, StreamExt};
use std::pin::Pin;

use crate::kernel::config::ProviderConfig;
use crate::kernel::event::KernelEvent;

// Use standardized types from SDK
pub use inference_sdk_core::{
    InferenceEvent, InferenceProvider, InferenceRequest, InferenceMessage, 
    InferenceRole, InferenceContent, ToolSpec, InferenceResult, Usage, SdkError, InferenceStream
};
use futures::future::BoxFuture;

/// Which LLM provider to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProviderKind {
    Anthropic,
    OpenAI,
    Mock,
}

impl FromStr for ProviderKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "anthropic" => Ok(ProviderKind::Anthropic),
            "openai" => Ok(ProviderKind::OpenAI),
            "mock" => Ok(ProviderKind::Mock),
            _ => anyhow::bail!("Unknown provider kind: {}", s),
        }
    }
}


/// Options for inference execution
#[derive(Debug, Clone, Default)]
pub struct InferenceOptions {
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub thinking_budget: Option<u32>,
}

/// A unified wrapper around provider-specific clients.
#[derive(Clone)]
pub struct ProviderClient {
    pub kind: ProviderKind,
    pub provider: std::sync::Arc<dyn InferenceProvider>,
}

impl ProviderClient {
    pub fn new(
        kind: ProviderKind,
        provider: std::sync::Arc<dyn InferenceProvider>,
    ) -> Self {
        Self { kind, provider }
    }

    /// Run a non-streaming completion (aggregates the stream).
    pub async fn completion(
        &self,
        model: &str,
        system_prompt: &str,
        messages: &[InferenceMessage],
    ) -> Result<String> {
        let req = self.build_request(model, system_prompt, messages, &[], &InferenceOptions::default());
        let result = self.provider.complete(req).await?;
        Ok(result.content)
    }

    /// Run a streaming completion.
    pub async fn stream(
        &self,
        model: &str,
        system_prompt: &str,
        messages: &[InferenceMessage],
        tools: &[serde_json::Value],
        options: &InferenceOptions,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<KernelEvent>> + Send>>> {
        let req = self.build_request(model, system_prompt, messages, tools, options);
        let sdk_stream = self.provider.stream(req).await?;

        // Map SDK InferenceEvents to Bedrock KernelEvents
        let kernel_stream = sdk_stream.map(|res| {
            match res {
                Ok(event) => map_sdk_event(event),
                Err(e) => Err(anyhow::anyhow!("Provider error: {}", e)),
            }
        });

        Ok(Box::pin(kernel_stream))
    }
    
    fn build_request(
        &self,
        model: &str,
        system_prompt: &str,
        messages: &[InferenceMessage],
        tools: &[serde_json::Value],
        options: &InferenceOptions,
    ) -> InferenceRequest {
        let sdk_tools: Vec<ToolSpec> = tools.iter().filter_map(|t| {
             Some(ToolSpec {
                name: t.get("name")?.as_str()?.to_string(),
                description: t.get("description").and_then(|d| d.as_str()).map(String::from),
                input_schema: t.get("input_schema").cloned().unwrap_or(serde_json::json!({"type": "object"})),
             })
        }).collect();

        InferenceRequest::builder()
            .model(model)
            .messages(messages.to_vec())
            .system(system_prompt)
            .maybe_tools(if sdk_tools.is_empty() { None } else { Some(sdk_tools) })
            .maybe_temperature(options.temperature)
            .maybe_max_tokens(options.max_tokens)
            .maybe_thinking_budget(options.thinking_budget)
            .build()
    }
}

// ─── Event Mapping ───────────────────────────────────────────────

fn map_sdk_event(event: InferenceEvent) -> Result<KernelEvent> {
    match event {
        InferenceEvent::MessageStart { role, model, .. } => Ok(KernelEvent::MessageStart { role, model }),
        InferenceEvent::MessageDelta { content } => Ok(KernelEvent::MessageDelta { content_delta: content }),
        InferenceEvent::ThinkingDelta { content } => Ok(KernelEvent::ThinkingDelta { thinking: content }),
        InferenceEvent::ToolCall { id, name, args } => Ok(KernelEvent::ToolCall { id, name, args }),
        InferenceEvent::MessageEnd { input_tokens, output_tokens } => Ok(KernelEvent::MessageEnd { role: "assistant".to_string(), input_tokens, output_tokens }),
        InferenceEvent::Error { message } => Err(anyhow::anyhow!("Provider stream error: {}", message)),
    }
}

// ─── Provider Creation ───────────────────────────────────────────

pub fn create_anthropic_client(provider_config: &ProviderConfig) -> Result<std::sync::Arc<dyn InferenceProvider>> {
     let env_var = provider_config.api_key_env.as_ref().context("API key environment variable not configured")?;
     let api_key = std::env::var(env_var).context("Missing API Key")?;
     let mut config = anthropic_sdk::ClientConfig::new(api_key)?;
     if let Some(url) = &provider_config.base_url { config = config.with_base_url(url); }
     
     let client = anthropic_sdk::Client::from_config(config)?;
     Ok(std::sync::Arc::new(client))
}


pub fn create_openai_client(provider_config: &ProviderConfig) -> Result<std::sync::Arc<dyn InferenceProvider>> {
     let env_var = provider_config.api_key_env.as_ref().context("API key environment variable not configured")?;
     let api_key = std::env::var(env_var).context("Missing API Key")?;
     let mut config = openai_sdk::ClientConfig::new(api_key)?;
     if let Some(url) = &provider_config.base_url { config = config.with_base_url(url); }
     
     let client = openai_sdk::Client::from_config(config)?;
     Ok(std::sync::Arc::new(client))
}

pub fn create_mock_client(config: &ProviderConfig) -> std::sync::Arc<dyn InferenceProvider> {
    let response = config.base_url.clone().unwrap_or_else(|| "Mock response".to_string());
    std::sync::Arc::new(MockProvider { response })
}

pub struct MockProvider {
    response: String,
}

impl InferenceProvider for MockProvider {
    fn complete<'a>(&'a self, _request: InferenceRequest) -> BoxFuture<'a, Result<InferenceResult, SdkError>> {
        let content = self.response.clone();
        Box::pin(async move {
            Ok(InferenceResult {
                content,
                usage: Usage { input_tokens: 0, output_tokens: 0 },
            })
        })
    }

    fn stream<'a>(&'a self, _request: InferenceRequest) -> BoxFuture<'a, Result<InferenceStream, SdkError>> {
        let content = self.response.clone();
        Box::pin(async move {
            let events = vec![
                Ok(InferenceEvent::MessageStart {
                    role: "assistant".to_string(),
                    model: "mock-model".to_string(),
                    provider_id: "mock".to_string(),
                }),
                Ok(InferenceEvent::MessageDelta { content }),
                Ok(InferenceEvent::MessageEnd { input_tokens: 10, output_tokens: 5 }),
            ];
            Ok(Box::pin(futures::stream::iter(events)) as InferenceStream)
        })
    }
}

