//! Shared HTTP client with timeouts and retry.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;

#[derive(Clone)]
pub struct Http {
    client: reqwest::Client,
    retries: u32,
}

impl Default for Http {
    fn default() -> Self {
        Http::new(Duration::from_secs(30), 4)
    }
}

impl Http {
    pub fn new(timeout: Duration, retries: u32) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(Duration::from_secs(10))
            .user_agent("pm-rs/0.1")
            .build()
            .expect("http client");
        Http { client, retries }
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// GET with retry on network errors, 429 and 5xx (exponential backoff from 1 s).
    pub async fn get_json<T: DeserializeOwned>(&self, url: &str, query: &[(&str, String)]) -> Result<T> {
        let mut wait = Duration::from_secs(1);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let res = self.client.get(url).query(query).send().await;
            let retryable = match res {
                Ok(r) if r.status().is_success() => {
                    let bytes = r.bytes().await.with_context(|| format!("reading {url}"))?;
                    return serde_json::from_slice(&bytes).with_context(|| format!("decoding {url}"));
                }
                Ok(r) => {
                    let st = r.status();
                    if !(st.as_u16() == 429 || st.is_server_error()) {
                        let body = r.text().await.unwrap_or_default();
                        bail!("GET {url} -> {st}: {}", body.chars().take(300).collect::<String>());
                    }
                    format!("status {st}")
                }
                Err(e) => format!("{e}"),
            };
            if attempt > self.retries {
                bail!("GET {url} failed after {attempt} attempts: {retryable}");
            }
            tracing::debug!(%url, attempt, %retryable, "retrying");
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(Duration::from_secs(30));
        }
    }
}
