//! Sequential GitHub reads with explicit budgets and optional response snapshots.

use crate::backfill_cache::ResponseCache;
use anyhow::{Context, Result, ensure};
use reqwest::{Client, Url, header::HeaderMap};
use serde_json::Value;
use std::{path::PathBuf, time::Duration};

pub(crate) struct GitHub {
    client: Client,
    pub(crate) base: Url,
    token: String,
    cache: Option<ResponseCache>,
    max_requests: u64,
    reserve: u64,
    pub(crate) requests: u64,
    pub(crate) cache_hits: u64,
    remaining: Option<u64>,
    reset: Option<u64>,
}

impl GitHub {
    pub(crate) async fn new(
        base: Url,
        token: String,
        cache_dir: Option<PathBuf>,
        max_requests: u64,
        reserve: u64,
    ) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .user_agent("github-actions-observer")
                .timeout(Duration::from_secs(60))
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()?,
            base,
            token,
            cache: match cache_dir {
                Some(directory) => Some(ResponseCache::open(directory).await?),
                None => None,
            },
            max_requests,
            reserve,
            requests: 0,
            cache_hits: 0,
            remaining: None,
            reset: None,
        })
    }

    pub(crate) fn report(&self) -> String {
        format!(
            "outbound_requests={}/{}, cache_hits={}, remaining={}, reserve={}, reset_unix={}",
            self.requests,
            self.max_requests,
            self.cache_hits,
            self.remaining
                .map_or_else(|| "unknown".into(), |value| value.to_string()),
            self.reserve,
            self.reset
                .map_or_else(|| "unknown".into(), |value| value.to_string())
        )
    }

    pub(crate) async fn get(&mut self, path: &str, parameters: &[(&str, String)]) -> Result<Value> {
        let mut url = self.base.join(path)?;
        if !parameters.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(parameters.iter().map(|(key, value)| (*key, value)));
        }
        if let Some(cache) = &self.cache
            && let Some(body) = cache.read(url.as_str()).await?
        {
            self.cache_hits += 1;
            return Ok(body);
        }
        if self.remaining.is_none() {
            let rate = self.send(self.base.join("rate_limit")?, true).await?;
            self.remaining = Some(
                rate.pointer("/resources/core/remaining")
                    .and_then(Value::as_u64)
                    .context("rate-limit preflight missing core remaining")?,
            );
            self.reset = Some(
                rate.pointer("/resources/core/reset")
                    .and_then(Value::as_u64)
                    .context("rate-limit preflight missing core reset")?,
            );
        }
        let body = self.send(url.clone(), false).await?;
        if let Some(cache) = &self.cache {
            cache.write(url.as_str(), &body).await?;
        }
        Ok(body)
    }

    async fn send(&mut self, url: Url, preflight: bool) -> Result<Value> {
        ensure!(
            self.requests < self.max_requests,
            "request budget reached; {}; resume with the same cache directory and a new invocation",
            self.report()
        );
        if !preflight {
            ensure!(
                self.remaining
                    .is_some_and(|remaining| remaining > self.reserve),
                "primary rate-limit reserve reached; {}; resume after reset (no automatic retry)",
                self.report()
            );
            self.remaining = self.remaining.map(|remaining| remaining.saturating_sub(1));
        }
        self.requests += 1;
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.token)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .send()
            .await
            .context("GitHub request failed; no retry was attempted")?;
        fn number(headers: &HeaderMap, name: &str) -> Option<u64> {
            headers.get(name)?.to_str().ok()?.parse().ok()
        }
        if let Some(remaining) = number(response.headers(), "x-ratelimit-remaining") {
            self.remaining = Some(remaining);
        }
        if let Some(reset) = number(response.headers(), "x-ratelimit-reset") {
            self.reset = Some(reset);
        }
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("not supplied")
            .to_owned();
        ensure!(
            response.status().is_success(),
            "GitHub returned {}; {}; Retry-After={retry_after}; no retry attempted. Honor Retry-After and primary reset; for secondary throttling without Retry-After wait at least one minute before resuming",
            response.status(),
            self.report()
        );
        response
            .json()
            .await
            .context("invalid GitHub JSON response")
    }
}
