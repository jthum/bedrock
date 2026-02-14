# Bedrock

**A single-binary, event-driven runtime for programmatic autonomy.**

Bedrock is an agentic substrate designed to be the "rock" that autonomous agents stand on. It strictly separates the **Physics of Execution** (Rust Kernel) from the **Law of Governance** (Lua Harness Scripts).

The Kernel has no opinions. Your harness has all of them.

---

## 🚀 Key Features

- **Physics vs. Harness**: Deterministic governance via embedded Luau (Roblox's sandboxed Lua dialect).
- **Single-Binary Rust Runtime**: Built for performance, safety, and zero-dependency distribution (~11MB).
- **Cognitive Memory**: Built-in semantic memory support using `sqlite-vec` in Turso.
- **Subagent Primitive**: Natively spawn nested kernel instances for recursive task delegation.
- **Model Context Protocol (MCP)**: Dynamic tool discovery and connection to external MCP servers.
- **Multi-Provider Architecture**: Use and switch between multiple named provider instances (e.g., "primary-claude", "backup-gpt") within the same session or for specific subagents.
- **Adaptive Thinking**: Full support for Anthropic's extended reasoning (Claude 3.7 Sonnet / Opus 4.6) with dynamic budget control.
- **Event-Driven Hub**: Every action (tool call, turn, token usage) flows through a programmable governance layer.
- **Persisted Context**: Atomic event logging and message history in a portable Turso/SQLite database.

---

## 🛠 Getting Started

### 1. Build
```bash
cargo build --release
```

### 2. Configure
Create a `bedrock.toml` or copy `bedrock.toml.example`. Ensure you set your API keys:
```bash
export ANTHROPIC_API_KEY="sk-..."
```

### 3. Run
```bash
# Start an interactive session
./target/release/bedrock repl

# Run a one-shot prompt
./target/release/bedrock run --prompt "Refactor this module and update the CHANGELOG."
```

---

## ⚖️ The Philosophy: Physics vs. Opinion

Most AI frameworks treat governance as a "prompting challenge." Bedrock treats it as an **Operating System challenge**.

1. **Inference (The Brain)** proposes an action.
2. **Harness (The Law)** decides if the action is legal.
3. **Kernel (The Physics)** executes the action and captures the result.

If a Lua harness script returns `REJECT`, the Kernel **physically cannot** execute the tool call.

---

## 📄 License

MIT (or your preferred license). See LICENSE for details.
