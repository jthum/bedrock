# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2026-02-14

### Added
- **Named Providers & Multi-Instance Support (Phase 20)**:
  - Supported arbitrary naming for provider instances in `bedrock.toml` (e.g., `[providers.my-fast-client]`).
  - Introduced `type` field in provider configuration to support multiple instances of the same provider kind.
  - Exposed `ctx.provider` setter in Lua `on_before_inference` hook for dynamic, mid-turn switching.
  - Refactored internal `Kernel` and `Harness` logic to resolve clients by their configured string names.

## [0.6.0] - 2026-02-14

### Changed
- **Capability Normalizer Architecture (Phase 15)**:
  - Refactored `ProviderClient` to be provider-agnostic by utilizing the `InferenceProvider` trait from the normalized SDK.
  - Standardized streaming event handling: Bedrock now consumes a unified `InferenceEvent` stream regardless of the backend (OpenAI or Anthropic).
  - Decoupled inference and embeddings logic: Embeddings are now handled through a dedicated `EmbeddingProvider` abstraction.
  - Simplified kernel-to-provider communication, removing thousands of lines of provider-specific boilerplate and mapping logic.

---

## [0.5.0] - 2026-02-13

### Added
- **Adaptive Thinking (Phase 11)**:
  - Full support for Anthropic's extended reasoning (Claude 3.7 Sonnet / Opus 4.6).
  - Exposure of `thinking_budget` to the Harness Engine for dynamic reasoning depth control.
- **Cognitive Memory & Anchorage (Phase 12)**:
  - Vector search primitives integrated via Turso/SQLite-vec.
  - Automated session summarization and fact anchorage via `on_task_complete` hooks.
- **Multi-Provider Support (Phase 18) & Mid-Turn Switching (Phase 19)**:
  - Enabled coexistence and switching between Anthropic and OpenAI within the same session.
  - Support for `ctx.provider` overrides in `bedrock.agent.spawn` and `on_before_inference`.

## [0.4.0] - 2026-02-13

### Added
- **MCP SDK Support**: Integrated a custom, lightweight Rust-based MCP SDK (`mcp-sdk-rust`) into Bedrock.
- **Dynamic Tool Loading**:
  - `bridge_mcp` tool: Allows agents to request spawning and connecting to external MCP servers.
  - `McpToolProxy`: Automatically registers tools from MCP servers as native Bedrock tools.
- **Ecosystem Stability Primitives**:
  - `on_task_complete` hook: Enables harnesses to validate state and re-queue tasks when the queue is exhausted.
  - `bedrock.context` module: New Lua global module providing `context.glob(pattern)` for safe, workspace-aware file discovery.
- **Internal Stability**:
  - Optimized binary size through LTO and symbol stripping (achieving ~11MB).
  - Hardened `run_task` loop with better error recovery and multi-turn consistency.

### Fixed
- Resolved multiple compilation errors related to borrow checking and brace nesting in the core Kernel.
- Fixed duplicate field declarations in the `Kernel` struct.

---

## [0.3.0] - 2026-02-13

### Added
- **Steerable Command Queue**: Added a per-session task queue in the Kernel allowing for persistent, asynchronous steering by humans or harnesses.
- **Interactive REPL**: New `bedrock repl` command for persistent conversational interaction with the workspace.
- **Task Decomposition Primitives**:
  - `submit_task` tool: Allows agents to propose a multi-step plan.
  - `on_task_submit` hook: Enables harnesses to intercept, approve, reject, or modify agent plans.
- **Verdict::Modify**: Extended the governance system to support data-carrying verdicts. Harnesses can now modify tool arguments (`on_tool_call`) or task lists (`on_task_submit`) on the fly.
- **Steering API**: `session.queue()`, `session.queue_next()`, and `session.clear_queue()` exposed to Lua for active control.

### Changed
- **Kernel Loop**: Refactored the core `run` loop to be queue-driven, supporting multiple sequential tasks within a single persistent session.
- **Harness Engine**: Updated `parse_verdict` to handle `MODIFY` verdict codes and associated JSON data.

---

## [0.2.0] - 2026-02-12

### Added
- **on_before_inference** hook: Enables context engineering and mutation before LLM calls.
- **on_agent_start** hook: Allows harness scripts to initialize state at session startup.
- **Session Globals**: `session.list(limit, offset)` and `session.load(id)` exposed to Lua.
- **Context Globals**: `ctx.summarize()`, `ctx.add_message()`, and `ctx.system_prompt` access.
- **Coding Agent**: Experimental `coding_agent.lua` for automatic `BEDROCK.md` injection.

### Changed
- **Synchronous Bridge**: Refactored `ctx.summarize` to be synchronous via `block_in_place`, ensuring compatibility with the synchronous Luau VM.
- **Inference Content**: Refactored `InferenceContent::Text` to a struct variant for better `serde` compatibility.
- **Sandboxing**: Enhanced harness loading with per-script environments to prevent global pollution.

---

## [0.1.0] - 2026-02-10

### Added
- **Core Engine**: Embedded Luau (mlua) runtime for sandboxed "Governance Harnesses."
- **Persistence**: Turso-backed `StateStore` for atomic event logging, message history, and tool execution tracking.
- **Tool Registry**: Extensible system for LLM-accessible tools with built-ins:
  - `read_file`, `write_file`, `edit_file` (safe workspace-restricted access).
  - `shell_exec` (with timeout and output truncation).
- **Governance Primitives**: `on_tool_call` (gating) and `on_token_usage` (budgeting) hooks.
- **CLI**: Interactive streaming UX with `--verbose` for debugging and `--json` for programmatic consumption.
- **SDK Integration**: Native adapters for Anthropic and OpenAI.

### Fixed
- Improved output truncation for large shell results to prevent context window overflows.
- Resolved Luau metatable indexing for property access on Context objects.
