//! turso-backed state store for Bedrock.
//!
//! Provides persistent storage for:
//! - Event log (append-only)
//! - Message history (per session)
//! - Tool execution log
//! - Harness key-value store
//! - Cognitive memories (vector store)

use anyhow::{Context, Result};
use turso::{Connection, Database};

/// The state store manages all Bedrock persistence.
#[derive(Clone)]
pub struct StateStore {
    db: Database,
    conn: Connection,
}

/// Schema version — bump when changing table structure.
const SCHEMA_VERSION: u32 = 1;

/// SQL statements to initialize the database schema.
const INIT_SCHEMA: &str = r#"
-- Core event log (append-only)
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Message history (per session)
CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,
    turn_index  INTEGER NOT NULL,
    role        TEXT NOT NULL,
    content     TEXT NOT NULL,
    token_count INTEGER,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Harness key-value store
CREATE TABLE IF NOT EXISTS harness_kv (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    expires_at  TEXT,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Tool execution log
CREATE TABLE IF NOT EXISTS tool_executions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id    TEXT NOT NULL,
    turn_index    INTEGER NOT NULL,
    tool_call_id  TEXT NOT NULL,
    tool_name     TEXT NOT NULL,
    args          TEXT NOT NULL,
    output        TEXT,
    is_error      INTEGER NOT NULL DEFAULT 0,
    duration_ms   INTEGER,
    verdict       TEXT NOT NULL DEFAULT 'allow',
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_info (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_tool_executions_session ON tool_executions(session_id);

-- Cognitive Memory
CREATE TABLE IF NOT EXISTS memories (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,
    content     TEXT NOT NULL,
    embedding   F32_BLOB(1536), 
    metadata    TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Vector index (DiskANN) if supported, or just verify column exists.
-- We use F32_BLOB which is compatible with vector functions.
"#;

impl StateStore {
    /// Open or create a state store at the given path.
    ///
    /// Creates parent directories and initializes the schema if the database is new.
    pub async fn open(db_path: &str) -> Result<Self> {
        // Create parent directories
        let path = std::path::Path::new(db_path);
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create database directory: {}", parent.display())
                })?;
            }
        }

        let db = turso::Builder::new_local(db_path)
            .build()
            .await
            .with_context(|| format!("Failed to open database: {}", db_path))?;

        let conn = db
            .connect()
            .with_context(|| "Failed to connect to database")?;

        let store = Self { db, conn };
        store.init_schema().await?;

        Ok(store)
    }

    /// Open an in-memory state store (useful for testing).
    pub async fn open_memory() -> Result<Self> {
        let db = turso::Builder::new_local(":memory:")
            .build()
            .await
            .with_context(|| "Failed to open in-memory database")?;

        let conn = db
            .connect()
            .with_context(|| "Failed to connect to in-memory database")?;

        let store = Self { db, conn };
        store.init_schema().await?;

        Ok(store)
    }

    /// Initialize the database schema.
    async fn init_schema(&self) -> Result<()> {
        // execute_batch handles multi-statement SQL natively
        self.conn
            .execute_batch(INIT_SCHEMA)
            .await
            .with_context(|| "Failed to initialize database schema")?;

        // Record schema version
        self.conn
            .execute(
                "INSERT OR REPLACE INTO schema_info (key, value) VALUES ('version', ?1)",
                [SCHEMA_VERSION.to_string()],
            )
            .await?;

        Ok(())
    }

    // ─── Event Log ───────────────────────────────────────────────

    /// Persist a KernelEvent to the event log.
    pub async fn insert_event(
        &self,
        session_id: &str,
        event_type: &str,
        payload: &serde_json::Value,
    ) -> Result<()> {
        let payload_str = serde_json::to_string(payload)?;
        self.conn
            .execute(
                "INSERT INTO events (session_id, event_type, payload) VALUES (?1, ?2, ?3)",
                turso::params![session_id, event_type, payload_str],
            )
            .await
            .with_context(|| format!("Failed to insert event for session: {}", session_id))?;
        Ok(())
    }

    /// Get all events for a session, ordered by creation time.
    pub async fn get_events(&self, session_id: &str) -> Result<Vec<EventRow>> {
        let mut rows = self
            .conn
            .query(
                "SELECT id, session_id, event_type, payload, created_at FROM events WHERE session_id = ?1 ORDER BY id",
                [session_id],
            )
            .await?;

        let mut events = Vec::new();
        while let Some(row) = rows.next().await? {
            events.push(EventRow {
                id: row.get::<i64>(0)?,
                session_id: row.get::<String>(1)?,
                event_type: row.get::<String>(2)?,
                payload: row.get::<String>(3)?,
                created_at: row.get::<String>(4)?,
            });
        }
        Ok(events)
    }

    /// List recent sessions, ordered by last activity.
    pub async fn list_sessions(&self, limit: usize, offset: usize) -> Result<Vec<String>> {
        let mut rows = self
            .conn
            .query(
                "SELECT session_id FROM events GROUP BY session_id ORDER BY MAX(id) DESC LIMIT ?1 OFFSET ?2",
                turso::params![limit as i64, offset as i64],
            )
            .await?;

        let mut sessions = Vec::new();
        while let Some(row) = rows.next().await? {
            sessions.push(row.get(0)?);
        }
        Ok(sessions)
    }

    // ─── Message History ─────────────────────────────────────────

    /// Insert a message into the history.
    pub async fn insert_message(
        &self,
        session_id: &str,
        turn_index: u32,
        role: &str,
        content: &serde_json::Value,
        token_count: Option<u32>,
    ) -> Result<()> {
        let content_str = serde_json::to_string(content)?;
        self.conn
            .execute(
                "INSERT INTO messages (session_id, turn_index, role, content, token_count) VALUES (?1, ?2, ?3, ?4, ?5)",
                turso::params![
                    session_id,
                    turn_index as i64,
                    role,
                    content_str,
                    token_count.map(|t| t as i64),
                ],
            )
            .await
            .with_context(|| format!("Failed to insert message for session: {}", session_id))?;
        Ok(())
    }

    /// Get all messages for a session.
    pub async fn get_messages(&self, session_id: &str) -> Result<Vec<MessageRow>> {
        let mut rows = self
            .conn
            .query(
                "SELECT id, session_id, turn_index, role, content, token_count, created_at FROM messages WHERE session_id = ?1 ORDER BY id",
                [session_id],
            )
            .await?;

        let mut messages = Vec::new();
        while let Some(row) = rows.next().await? {
            messages.push(MessageRow {
                id: row.get::<i64>(0)?,
                session_id: row.get::<String>(1)?,
                turn_index: row.get::<i64>(2)? as u32,
                role: row.get::<String>(3)?,
                content: row.get::<String>(4)?,
                token_count: row.get::<Option<i64>>(5)?.map(|t| t as u32),
                created_at: row.get::<String>(6)?,
            });
        }
        Ok(messages)
    }

    // ─── Tool Executions ─────────────────────────────────────────

    /// Log a tool execution.
    pub async fn insert_tool_execution(
        &self,
        session_id: &str,
        turn_index: u32,
        tool_call_id: &str,
        tool_name: &str,
        args: &serde_json::Value,
        output: Option<&str>,
        is_error: bool,
        duration_ms: Option<u64>,
        verdict: &str,
    ) -> Result<()> {
        let args_str = serde_json::to_string(args)?;
        self.conn
            .execute(
                "INSERT INTO tool_executions (session_id, turn_index, tool_call_id, tool_name, args, output, is_error, duration_ms, verdict) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                turso::params![
                    session_id,
                    turn_index as i64,
                    tool_call_id,
                    tool_name,
                    args_str,
                    output,
                    is_error as i64,
                    duration_ms.map(|d| d as i64),
                    verdict,
                ],
            )
            .await
            .with_context(|| format!("Failed to insert tool execution for session: {}", session_id))?;
        Ok(())
    }

    /// Get all tool executions for a session.
    pub async fn get_tool_executions(&self, session_id: &str) -> Result<Vec<ToolExecutionRow>> {
        let mut rows = self
            .conn
            .query(
                "SELECT id, session_id, turn_index, tool_call_id, tool_name, args, output, is_error, duration_ms, verdict, created_at FROM tool_executions WHERE session_id = ?1 ORDER BY id",
                [session_id],
            )
            .await?;

        let mut execs = Vec::new();
        while let Some(row) = rows.next().await? {
            execs.push(ToolExecutionRow {
                id: row.get::<i64>(0)?,
                session_id: row.get::<String>(1)?,
                turn_index: row.get::<i64>(2)? as u32,
                tool_call_id: row.get::<String>(3)?,
                tool_name: row.get::<String>(4)?,
                args: row.get::<String>(5)?,
                output: row.get::<Option<String>>(6)?,
                is_error: row.get::<i64>(7)? != 0,
                duration_ms: row.get::<Option<i64>>(8)?.map(|d| d as u64),
                verdict: row.get::<String>(9)?,
                created_at: row.get::<String>(10)?,
            });
        }
        Ok(execs)
    }

    // ─── Memories (Vector Store) ─────────────────────────────────

    /// Insert a memory with an embedding vector.
    pub async fn insert_memory(
        &self,
        session_id: &str,
        content: &str,
        vector: &[f32],
        metadata: &serde_json::Value,
    ) -> Result<()> {
        // Convert vector to raw bytes (little endian)
        let mut vector_bytes = Vec::with_capacity(vector.len() * 4);
        for &val in vector {
            vector_bytes.extend_from_slice(&val.to_le_bytes());
        }

        let metadata_str = serde_json::to_string(metadata)?;

        self.conn
            .execute(
                "INSERT INTO memories (session_id, content, embedding, metadata) VALUES (?1, ?2, ?3, ?4)",
                turso::params![
                    session_id,
                    content,
                    vector_bytes,
                    metadata_str,
                ],
            )
            .await
            .with_context(|| format!("Failed to insert memory for session: {}", session_id))?;
        Ok(())
    }

    /// Search memories by semantic similarity.
    pub async fn search_memories(
        &self,
        session_id: &str,
        vector: &[f32],
        limit: usize,
    ) -> Result<Vec<MemoryRow>> {
         // Convert target vector to bytes
        let mut vector_bytes = Vec::with_capacity(vector.len() * 4);
        for &val in vector {
            vector_bytes.extend_from_slice(&val.to_le_bytes());
        }

        let mut rows = self.conn.query(
            "SELECT id, session_id, content, metadata, created_at, vector_distance_cos(embedding, ?1) as distance 
             FROM memories 
             WHERE session_id = ?2 
             ORDER BY distance ASC 
             LIMIT ?3",
            turso::params![vector_bytes, session_id, limit as i64],
        ).await.context("Failed to search memories (ensure vector extension is loaded?)")?;

        let mut memories = Vec::new();
        while let Some(row) = rows.next().await? {
             memories.push(MemoryRow {
                id: row.get(0)?,
                session_id: row.get(1)?,
                content: row.get(2)?,
                metadata: row.get(3)?,
                created_at: row.get(4)?,
                score: 1.0 - row.get::<f64>(5)?, // Convert distance to similarity
             });
        }
        
        Ok(memories)
    }

    // ─── Harness KV Store ────────────────────────────────────────

    /// Set a key-value pair in the harness store.
    pub async fn kv_set(&self, key: &str, value: &str) -> Result<()> {
        const MAX_KV_VALUE_SIZE: usize = 1_048_576; // 1MB

        if value.len() > MAX_KV_VALUE_SIZE {
            anyhow::bail!(
                "KV value exceeds maximum size of {} bytes (got {})",
                MAX_KV_VALUE_SIZE,
                value.len()
            );
        }

        self.conn
            .execute(
                "INSERT OR REPLACE INTO harness_kv (key, value, updated_at) VALUES (?1, ?2, datetime('now'))",
                turso::params![key, value],
            )
            .await
            .with_context(|| format!("Failed to set KV pair for key: {}", key))?;
        Ok(())
    }

    /// Get a value from the harness store.
    pub async fn kv_get(&self, key: &str) -> Result<Option<String>> {
        let mut rows = self
            .conn
            .query(
                "SELECT value FROM harness_kv WHERE key = ?1 AND (expires_at IS NULL OR expires_at > datetime('now'))",
                [key],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row.get::<String>(0)?))
        } else {
            Ok(None)
        }
    }

    /// Delete a key from the harness store.
    pub async fn kv_delete(&self, key: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM harness_kv WHERE key = ?1", [key])
            .await?;
        Ok(())
    }

    /// Get the database connection (for advanced operations).
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Get the underlying database (for advanced ops, e.g. shutdown).
    #[allow(dead_code)]
    pub fn database(&self) -> &Database {
        &self.db
    }
}

// ─── Row Types ───────────────────────────────────────────────

/// A row from the `events` table.
#[derive(Debug, Clone)]
pub struct EventRow {
    pub id: i64,
    pub session_id: String,
    pub event_type: String,
    pub payload: String,
    pub created_at: String,
}

/// A row from the `messages` table.
#[derive(Debug, Clone)]
pub struct MessageRow {
    pub id: i64,
    pub session_id: String,
    pub turn_index: u32,
    pub role: String,
    pub content: String,
    pub token_count: Option<u32>,
    pub created_at: String,
}

/// A row from the `tool_executions` table.
#[derive(Debug, Clone)]
pub struct ToolExecutionRow {
    pub id: i64,
    pub session_id: String,
    pub turn_index: u32,
    pub tool_call_id: String,
    pub tool_name: String,
    pub args: String,
    pub output: Option<String>,
    pub is_error: bool,
    pub duration_ms: Option<u64>,
    pub verdict: String,
    pub created_at: String,
}

/// A row from the `memories` table.
#[derive(Debug, Clone)]
pub struct MemoryRow {
    pub id: i64,
    pub session_id: String,
    pub content: String,
    pub metadata: String,
    pub created_at: String,
    pub score: f64,
}

// ─── Tests ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_schema_initialization() {
        let store = StateStore::open_memory().await.unwrap();

        // Check schema version
        let mut rows = store
            .conn
            .query("SELECT value FROM schema_info WHERE key = 'version'", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let version: String = row.get(0).unwrap();
        assert_eq!(version, SCHEMA_VERSION.to_string());
    }

    #[tokio::test]
    async fn test_insert_and_get_events() {
        let store = StateStore::open_memory().await.unwrap();
        let session = "test-session-1";

        store
            .insert_event(session, "agent_start", &json!({"session_id": session}))
            .await
            .unwrap();
        store
            .insert_event(session, "turn_start", &json!({"turn_index": 0}))
            .await
            .unwrap();

        let events = store.get_events(session).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "agent_start");
        assert_eq!(events[1].event_type, "turn_start");
    }

    #[tokio::test]
    async fn test_events_isolated_by_session() {
        let store = StateStore::open_memory().await.unwrap();

        store
            .insert_event("session-a", "agent_start", &json!({}))
            .await
            .unwrap();
        store
            .insert_event("session-b", "agent_start", &json!({}))
            .await
            .unwrap();

        let events_a = store.get_events("session-a").await.unwrap();
        let events_b = store.get_events("session-b").await.unwrap();
        assert_eq!(events_a.len(), 1);
        assert_eq!(events_b.len(), 1);
    }

    #[tokio::test]
    async fn test_insert_and_get_messages() {
        let store = StateStore::open_memory().await.unwrap();
        let session = "test-session";

        store
            .insert_message(session, 0, "user", &json!([{"type": "text", "text": "hello"}]), None)
            .await
            .unwrap();
        store
            .insert_message(session, 0, "assistant", &json!([{"type": "text", "text": "hi!"}]), Some(10))
            .await
            .unwrap();

        let msgs = store.get_messages(session).await.unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].role, "user");
        assert_eq!(msgs[1].role, "assistant");
        assert_eq!(msgs[1].token_count, Some(10));
    }

    #[tokio::test]
    async fn test_insert_and_get_tool_executions() {
        let store = StateStore::open_memory().await.unwrap();
        let session = "test-session";

        store
            .insert_tool_execution(
                session,
                0,
                "call_1",
                "read_file",
                &json!({"path": "main.rs"}),
                Some("fn main() {}"),
                false,
                Some(15),
                "allow",
            )
            .await
            .unwrap();

        let execs = store.get_tool_executions(session).await.unwrap();
        assert_eq!(execs.len(), 1);
        assert_eq!(execs[0].tool_name, "read_file");
        assert_eq!(execs[0].output, Some("fn main() {}".to_string()));
        assert!(!execs[0].is_error);
        assert_eq!(execs[0].duration_ms, Some(15));
        assert_eq!(execs[0].verdict, "allow");
    }

    #[tokio::test]
    async fn test_tool_execution_with_error() {
        let store = StateStore::open_memory().await.unwrap();
        let session = "test-session";

        store
            .insert_tool_execution(
                session,
                1,
                "call_2",
                "shell_exec",
                &json!({"command": "rm -rf /"}),
                Some("Permission denied"),
                true,
                Some(5),
                "reject",
            )
            .await
            .unwrap();

        let execs = store.get_tool_executions(session).await.unwrap();
        assert_eq!(execs.len(), 1);
        assert!(execs[0].is_error);
        assert_eq!(execs[0].verdict, "reject");
    }

    #[tokio::test]
    async fn test_kv_set_get_delete() {
        let store = StateStore::open_memory().await.unwrap();

        // Set
        store.kv_set("budget_remaining", "1000").await.unwrap();

        // Get
        let val = store.kv_get("budget_remaining").await.unwrap();
        assert_eq!(val, Some("1000".to_string()));

        // Update
        store.kv_set("budget_remaining", "500").await.unwrap();
        let val = store.kv_get("budget_remaining").await.unwrap();
        assert_eq!(val, Some("500".to_string()));

        // Delete
        store.kv_delete("budget_remaining").await.unwrap();
        let val = store.kv_get("budget_remaining").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_kv_get_nonexistent() {
        let store = StateStore::open_memory().await.unwrap();
        let val = store.kv_get("nonexistent").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_file_based_store() {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let db_path_str = db_path.to_str().unwrap();

        // Create and populate
        {
            let store = StateStore::open(db_path_str).await.unwrap();
            store
                .insert_event("s1", "agent_start", &json!({}))
                .await
                .unwrap();
            store.kv_set("key1", "value1").await.unwrap();
        }

        // Reopen and verify persistence
        {
            let store = StateStore::open(db_path_str).await.unwrap();
            let events = store.get_events("s1").await.unwrap();
            assert_eq!(events.len(), 1);

            let val = store.kv_get("key1").await.unwrap();
            assert_eq!(val, Some("value1".to_string()));
        }
    }
}
