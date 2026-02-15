use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};


/// Top-level Bedrock configuration, parsed from `bedrock.toml`.
#[derive(Debug, Clone, Deserialize)]
pub struct BedrockConfig {
    pub agent: AgentConfig,
    #[serde(default)]
    pub kernel: KernelConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub harness: HarnessConfig,
    #[serde(default)]
    pub providers: ProvidersConfig,
    #[serde(default)]
    pub embeddings: Option<EmbeddingConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EmbeddingConfig {
    OpenAI,
    NoOp,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    /// System prompt for the LLM
    #[serde(default = "default_system_prompt")]
    pub system_prompt: String,
    /// Model identifier (e.g., "claude-sonnet-4-20250514", "gpt-4o")
    pub model: String,
    /// Provider name ("anthropic" or "openai")
    pub provider: String,
    /// Extended thinking configuration
    #[serde(default)]
    pub thinking: Option<ThinkingConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThinkingConfig {
    pub enabled: bool,
    pub budget_tokens: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KernelConfig {
    /// Root directory for workspace-relative paths
    #[serde(default = "default_workspace_root")]
    pub workspace_root: String,
    /// Maximum turns before the agent loop exits
    #[serde(default = "default_max_turns")]
    pub max_turns: u32,
    /// Heartbeat interval in seconds
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u32,
}

impl Default for KernelConfig {
    fn default() -> Self {
        Self {
            workspace_root: default_workspace_root(),
            max_turns: default_max_turns(),
            heartbeat_interval_secs: default_heartbeat_interval(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PersistenceConfig {
    /// Path to the libSQL database file
    #[serde(default = "default_database_path")]
    pub database_path: String,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            database_path: default_database_path(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HarnessConfig {
    /// Directory containing harness Lua scripts
    #[serde(default = "default_harness_directory")]
    pub directory: String,
    /// Root directory for harness `fs.*` functions (default: workspace root).
    /// Set to "/" for unrestricted filesystem access.
    #[serde(default = "default_harness_fs_root")]
    pub fs_root: String,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            directory: default_harness_directory(),
            fs_root: default_harness_fs_root(),
        }
    }
}

pub type ProvidersConfig = std::collections::HashMap<String, ProviderConfig>;

#[derive(Debug, Clone, Deserialize)]
pub struct ProviderConfig {
    /// The type of provider ("anthropic", "openai", "mock")
    #[serde(rename = "type")]
    pub kind: String,
    /// Environment variable name containing the API key
    pub api_key_env: Option<String>,
    /// Optional base URL override (for proxies)
    pub base_url: Option<String>,
}

// ─── Defaults ────────────────────────────────────────────────────

fn default_system_prompt() -> String {
    "You are a helpful coding assistant.".to_string()
}

fn default_workspace_root() -> String {
    ".".to_string()
}

fn default_max_turns() -> u32 {
    50
}

fn default_heartbeat_interval() -> u32 {
    30
}

fn default_database_path() -> String {
    ".bedrock/state.db".to_string()
}

fn default_harness_directory() -> String {
    ".bedrock/harnesses".to_string()
}

fn default_harness_fs_root() -> String {
    ".".to_string()
}

// ─── Loading ─────────────────────────────────────────────────────

impl BedrockConfig {
    /// Load configuration from a TOML file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Could not read config file: {}", path.display()))?;
        Self::from_str(&contents)
    }

    /// Parse configuration from a TOML string.
    pub fn from_str(toml_str: &str) -> Result<Self> {
        let config: BedrockConfig = toml::from_str(toml_str)
            .with_context(|| "Failed to parse bedrock.toml")?;
        config.validate()?;
        Ok(config)
    }

    /// Validate semantic invariants that serde can't enforce.
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.agent.model.trim().is_empty(),
            "agent.model must not be empty"
        );
        if !self.providers.contains_key(&self.agent.provider) {
            anyhow::bail!(
                "Provider '{}' configured in [agent] but not found in [providers]",
                self.agent.provider
            );
        }
        anyhow::ensure!(
            self.kernel.max_turns > 0,
            "kernel.max_turns must be greater than 0"
        );
        anyhow::ensure!(
            self.kernel.heartbeat_interval_secs > 0,
            "kernel.heartbeat_interval_secs must be greater than 0"
        );
        Ok(())
    }

    /// Resolve the workspace root path relative to a base directory.
    pub fn resolve_workspace_root(&self, base: &Path) -> PathBuf {
        let root = Path::new(&self.kernel.workspace_root);
        if root.is_absolute() {
            root.to_path_buf()
        } else {
            base.join(root)
        }
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            system_prompt: default_system_prompt(),
            model: "test-model".to_string(),
            provider: "mock".to_string(),
            thinking: None,
        }
    }
}

impl Default for BedrockConfig {
    fn default() -> Self {
        Self {
            agent: AgentConfig::default(),
            kernel: KernelConfig::default(),
            persistence: PersistenceConfig::default(),
            harness: HarnessConfig::default(),
            providers: ProvidersConfig::default(),
            embeddings: None,
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
[agent]
system_prompt = "You are a helpful coding assistant."
model = "claude-sonnet-4-20250514"
provider = "anthropic"

[agent.thinking]
enabled = false

[kernel]
workspace_root = "."
max_turns = 50
heartbeat_interval_secs = 30

[persistence]
database_path = ".bedrock/state.db"

[harness]
directory = ".bedrock/harnesses"

[providers.anthropic]
type = "anthropic"
api_key_env = "ANTHROPIC_API_KEY"

[providers.openai]
type = "openai"
api_key_env = "OPENAI_API_KEY"
"#;

        let config = BedrockConfig::from_str(toml).unwrap();
        assert_eq!(config.agent.model, "claude-sonnet-4-20250514");
        assert_eq!(config.agent.provider, "anthropic");
        assert_eq!(config.kernel.max_turns, 50);
        assert_eq!(config.persistence.database_path, ".bedrock/state.db");
        assert_eq!(config.harness.directory, ".bedrock/harnesses");
        assert_eq!(
            config.providers.get("anthropic").unwrap().api_key_env.as_ref().unwrap(),
            "ANTHROPIC_API_KEY"
        );
    }

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"
[agent]
model = "gpt-4o"
provider = "openai"

[providers.openai]
type = "openai"
"#;

        let config = BedrockConfig::from_str(toml).unwrap();
        assert_eq!(config.agent.model, "gpt-4o");
        assert_eq!(config.agent.provider, "openai");
        // Defaults should be applied
        assert_eq!(config.kernel.workspace_root, ".");
        assert_eq!(config.kernel.max_turns, 50);
        assert_eq!(config.persistence.database_path, ".bedrock/state.db");
        assert_eq!(config.harness.directory, ".bedrock/harnesses");
    }

    #[test]
    fn test_parse_with_base_url_override() {
        let toml = r#"
[agent]
model = "claude-sonnet-4-20250514"
provider = "anthropic"

[providers.anthropic]
type = "anthropic"
api_key_env = "ANTHROPIC_API_KEY"
base_url = "https://my-proxy.example.com/v1"
"#;

        let config = BedrockConfig::from_str(toml).unwrap();
        let provider = config.providers.get("anthropic").unwrap();
        assert_eq!(
            provider.base_url.as_ref().unwrap(),
            "https://my-proxy.example.com/v1"
        );
    }

    #[test]
    fn test_resolve_workspace_root_relative() {
        let toml = r#"
[agent]
model = "test"
provider = "anthropic"

[providers.anthropic]
type = "anthropic"

[kernel]
workspace_root = "src"
"#;

        let config = BedrockConfig::from_str(toml).unwrap();
        let resolved = config.resolve_workspace_root(Path::new("/home/user/project"));
        assert_eq!(resolved, PathBuf::from("/home/user/project/src"));
    }

    #[test]
    fn test_resolve_workspace_root_absolute() {
        let toml = r#"
[agent]
model = "test"
provider = "anthropic"

[providers.anthropic]
type = "anthropic"

[kernel]
workspace_root = "/absolute/path"
"#;

        let config = BedrockConfig::from_str(toml).unwrap();
        let resolved = config.resolve_workspace_root(Path::new("/home/user/project"));
        assert_eq!(resolved, PathBuf::from("/absolute/path"));
    }

    #[test]
    fn test_validate_empty_model() {
        let toml = r#"
[agent]
model = ""
provider = "anthropic"
"#;
        assert!(BedrockConfig::from_str(toml).is_err());
    }

    #[test]
    fn test_validate_invalid_provider() {
        let toml = r#"
[agent]
model = "gpt-4o"
provider = "google"
"#;
        let err = BedrockConfig::from_str(toml).unwrap_err();
        assert!(err.to_string().contains("google"));
    }

    #[test]
    fn test_validate_zero_max_turns() {
        let toml = r#"
[agent]
model = "gpt-4o"
provider = "openai"

[kernel]
max_turns = 0
"#;
        assert!(BedrockConfig::from_str(toml).is_err());
    }
}
