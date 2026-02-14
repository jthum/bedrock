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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

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

            if verbose {
                eprintln!("[bedrock] Config loaded:");
                eprintln!("  model: {}", config.agent.model);
                eprintln!("  provider: {}", config.agent.provider);
                eprintln!("  workspace: {}", config.kernel.workspace_root);
                eprintln!("  harness_dir: {}", config.harness.directory);
                eprintln!("  db: {}", config.persistence.database_path);
                eprintln!();
            }

            // Build kernel, initialize state store, and run
            let mut kernel = Kernel::new(config, verbose, json);
            kernel.init_state().await?;
            kernel.init_clients()?;
            kernel.init_harness().await?;
            kernel.start_watcher()?;
            kernel.run(Some(prompt)).await?;

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

            if verbose {
                eprintln!("[bedrock] Config loaded (REPL mode):");
                eprintln!("  model: {}", config.agent.model);
                eprintln!("  provider: {}", config.agent.provider);
                eprintln!();
            }

            // Build kernel
            let mut kernel = Kernel::new(config, verbose, false); // JSON not supported in REPL yet
            kernel.init_state().await?;
            kernel.init_clients()?;
            kernel.init_harness().await?;
            kernel.start_watcher()?;
            
            // Start REPL loop
            let mut rl = DefaultEditor::new()?;
            if verbose {
                eprintln!("[bedrock] REPL started. Type 'exit' or Ctrl+D to quit.");
            } else {
                 println!("Bedrock REPL v0.6.0");
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
                            eprintln!("[bedrock] Reloading harness...");
                            match kernel.reload_harness().await {
                                Ok(_) => eprintln!("[bedrock] Harness reloaded successfully."),
                                Err(e) => eprintln!("[bedrock] Failed to reload harness: {}", e),
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
            let mut kernel = Kernel::new(config, true, false);
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
