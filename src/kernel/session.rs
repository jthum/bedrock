use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use mcp_sdk::client::McpClient;
use mcp_sdk::transport::StdioTransport;

use crate::inference::provider::InferenceMessage;
use crate::kernel::event::KernelEvent;

/// Holds the state of an active agent session.
pub struct SessionState {
    pub id: String,
    pub history: Vec<InferenceMessage>,
    pub queue: Arc<Mutex<VecDeque<String>>>,
    pub turn_index: u32,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub mcp_clients: Vec<Arc<McpClient<StdioTransport>>>,
    // Event channel for this session
    pub event_tx: mpsc::UnboundedSender<(String, KernelEvent)>,
    pub event_rx: Option<Arc<Mutex<Option<mpsc::UnboundedReceiver<(String, KernelEvent)>>>>>, // Kept for init, usually taken by Kernel
    pub event_task: Option<Arc<Mutex<Option<JoinHandle<()>>>>>,
}

impl SessionState {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            history: Vec::new(),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            turn_index: 0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            mcp_clients: Vec::new(),
            event_tx: tx,
            event_rx: Some(Arc::new(Mutex::new(Some(rx)))),
            event_task: Some(Arc::new(Mutex::new(None))),
        }
    }
}
