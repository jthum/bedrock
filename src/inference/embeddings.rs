use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A vector embedding of a text string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Embedding {
    /// The original text content.
    pub content: String,
    /// The vector representation.
    pub vector: Vec<f32>,
    /// The model used to generate the embedding.
    pub model: String,
}

/// Start of the embedding provider trait.
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Generate an embedding for the given text.
    async fn embed(&self, text: &str) -> Result<Embedding>;

    /// Generate embeddings for a batch of texts.
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Embedding>> {
        let mut embeddings = Vec::new();
        for text in texts {
            embeddings.push(self.embed(text).await?);
        }
        Ok(embeddings)
    }
}

/// Configuration for embedding providers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EmbeddingConfig {
    OpenAI {
        api_key: String,
        model: String,
    },
    // Placeholder for future local/other providers
    NoOp,
}

/// OpenAI embedding provider.
pub struct OpenAIEmbeddingProvider {
    client: openai_sdk::Client,
    model: String,
}

impl OpenAIEmbeddingProvider {
    pub fn new(api_key: String, model: String) -> Self {
        Self {
            client: openai_sdk::Client::new(api_key).expect("Failed to create OpenAI client"),
            model,
        }
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAIEmbeddingProvider {
    async fn embed(&self, text: &str) -> Result<Embedding> {
        let request = openai_sdk::EmbeddingRequest::builder()
            .input(text.to_string())
            .model(self.model.clone())
            .build();

        let response = self.client.embeddings().create(request).await?;
        
        if let Some(data) = response.data.first() {
             Ok(Embedding {
                content: text.to_string(),
                vector: data.embedding.clone(),
                model: self.model.clone(),
            })
        } else {
            anyhow::bail!("No embedding data returned from OpenAI")
        }
    }
}

/// No-op provider for testing or disabled embeddings.
pub struct NoOpEmbeddingProvider;

#[async_trait]
impl EmbeddingProvider for NoOpEmbeddingProvider {
    async fn embed(&self, text: &str) -> Result<Embedding> {
        Ok(Embedding {
            content: text.to_string(),
            vector: vec![0.001; 1536], // Non-zero for cosine similarity safety
            model: "noop".to_string(),
        })
    }
}

pub fn create_embedding_provider(config: &EmbeddingConfig) -> Box<dyn EmbeddingProvider> {
    match config {
        EmbeddingConfig::OpenAI { api_key, model } => {
            Box::new(OpenAIEmbeddingProvider::new(api_key.clone(), model.clone()))
        }
        EmbeddingConfig::NoOp => Box::new(NoOpEmbeddingProvider),
    }
}
