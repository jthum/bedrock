use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use bedrock::kernel::config::BedrockConfig;
use bedrock::kernel::Kernel;

/// Bedrock: A single-binary, event-driven LLM execution runtime
#[derive(Parser, Debug)]
#[command(name = "bedrock", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, default_value = "info", global = true)]
    log_level: String,

    /// Path to log file
    #[arg(long, global = true)]
    log_file: Option<PathBuf>,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    /// Run the agent with a prompt
    Run {
        /// The prompt to send to the LLM
        #[arg(long)]
        prompt: String,

        /// Path to bedrock.toml config file
        #[arg(long, default_value = "bedrock.toml")]
        config: PathBuf,

        /// Override the model from config
        #[arg(long)]
        model: Option<String>,

        /// Override the provider from config
        #[arg(long)]
        provider: Option<String>,

        /// Show verbose event-level output
        #[arg(long)]
        verbose: bool,

        /// Output events as NDJSON to stdout
        #[arg(long)]
        json: bool,
    },
    
    /// Start an interactive REPL session
    Repl {
        /// Path to bedrock.toml config file
        #[arg(long, default_value = "bedrock.toml")]
        config: PathBuf,

        /// Override the model from config
        #[arg(long)]
        model: Option<String>,

        /// Override the provider from config
        #[arg(long)]
        provider: Option<String>,

        /// Show verbose event-level output
        #[arg(long)]
        verbose: bool,
    },

    /// Run a specific harness script (for testing)
    Script {
        /// Path to the Lua script to run
        path: PathBuf,

        /// Path to bedrock.toml config file
        #[arg(long, default_value = "bedrock.toml")]
        config: PathBuf,

        /// Override the model from config
        #[arg(long)]
        model: Option<String>,

        /// Override the provider from config
        #[arg(long)]
        provider: Option<String>,
    },
}

use tracing_subscriber::{fmt, prelude::*, EnvFilter};

fn init_tracing(log_level: &str, log_file: Option<PathBuf>) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let stdout_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(true);

    let file_layer = log_file.map(|path| {
        let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let filename = path.file_name().unwrap_or_default();
        let file_appender = tracing_appender::rolling::never(parent, filename);
        fmt::layer()
            .with_writer(file_appender)
            .with_ansi(false)
            .json()
    });

    tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(file_layer)
        .init();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_level, cli.log_file)?;

    match cli.command {
        Commands::Run {
            prompt,
            config,
            model,
            provider,
            verbose,
            json,
        } => {
            // Load config
            let mut config = BedrockConfig::from_file(&config)
                .with_context(|| "Failed to load config")?;

            // Apply CLI overrides
            if let Some(m) = model {
                config.agent.model = m;
            }
            if let Some(p) = provider {
                config.agent.provider = p;
                // Re-validate after override
                config.validate()?;
            }

            tracing::info!(
                model = %config.agent.model,
                provider = %config.agent.provider,
                workspace = %config.kernel.workspace_root,
                harness_dir = %config.harness.directory,
                db = %config.persistence.database_path,
                "Config loaded"
            );

            // Build kernel, initialize state store, and run
            let mut kernel = Kernel::new(config, json);
            kernel.init_state().await?;
            kernel.init_clients()?;
            kernel.init_harness().await?;
            kernel.start_watcher()?;
            kernel.run(Some(prompt)).await?;
            kernel.end_session().await?;

            Ok(())
        }
        Commands::Repl {
            config,
            model,
            provider,
            verbose,
        } => {
            // Load config
            let mut config = BedrockConfig::from_file(&config)
                .with_context(|| "Failed to load config")?;

            // Apply CLI overrides
            if let Some(m) = model {
                config.agent.model = m;
            }
            if let Some(p) = provider {
                config.agent.provider = p;
                config.validate()?;
            }

            tracing::info!(
                model = %config.agent.model,
                provider = %config.agent.provider,
                "Config loaded (REPL mode)"
            );

            // Build kernel
            let mut kernel = Kernel::new(config, false); // JSON not supported in REPL yet
            kernel.init_state().await?;
            kernel.init_clients()?;
            kernel.init_harness().await?;
            kernel.start_watcher()?;
            
            // Start REPL loop
            let mut rl = DefaultEditor::new()?;
            tracing::info!("REPL started. Type 'exit' or Ctrl+D to quit.");
            if !verbose {
                 println!("Bedrock REPL v{}", env!("CARGO_PKG_VERSION"));
                 println!("Type 'exit' or Ctrl+D to quit. Type '/reload' to reload harness.");
            }

            // Trigger AgentStart
            kernel.run(None).await?;

            loop {
                let readline = rl.readline(">> ");
                match readline {
                    Ok(line) => {
                        let line = line.trim();
                        if line.is_empty() { continue; }
                        if line.eq_ignore_ascii_case("exit") { break; }
                        
                        if line.eq_ignore_ascii_case("/reload") {
                            tracing::info!("Reloading harness...");
                            match kernel.reload_harness().await {
                                Ok(_) => tracing::info!("Harness reloaded successfully."),
                                Err(e) => tracing::error!(error = %e, "Failed to reload harness"),
                            }
                            continue;
                        }
                        let _ = rl.add_history_entry(line);
                        
                        // Push prompt to kernel queue and run until empty
                        kernel.run(Some(line.to_string())).await?;
                    },
                    Err(ReadlineError::Interrupted) => {
                        println!("^C");
                        break;
                    },
                    Err(ReadlineError::Eof) => {
                         println!("^D");
                        break;
                    },
                    Err(err) => {
                        println!("Error: {:?}", err);
                        break;
                    }
                }
                }
            }
            kernel.end_session().await?;
            Ok(())
        }
        Commands::Script { path, config, model, provider } => {
             // Load config
            let mut config = BedrockConfig::from_file(&config)
                .with_context(|| "Failed to load config")?;

            // Apply CLI overrides
            if let Some(m) = model {
                config.agent.model = m;
            }
            if let Some(p) = provider {
                config.agent.provider = p;
                config.validate()?;
            }

            // Build kernel
            let mut kernel = Kernel::new(config, false);
            kernel.init_state().await?;
            kernel.init_clients()?;
            kernel.init_harness().await?;

            // Read script
            let script_content = std::fs::read_to_string(&path)
                .with_context(|| format!("Failed to read script: {}", path.display()))?;

            kernel.run_script(&script_content).await?;
            
            Ok(())
        }
    }
}
