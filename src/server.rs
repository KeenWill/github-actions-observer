use crate::{
    config::Filters,
    store::{self, Outcome, Source},
};
use anyhow::{Result, ensure};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
    routing::{get, post},
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use sqlx::PgPool;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

const MAX_PAYLOAD_BYTES: usize = 25 * 1024 * 1024;
const MAX_HEADER_BYTES: usize = 256;

#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    duplicate: AtomicU64,
    filtered: AtomicU64,
    rejected: AtomicU64,
    failed: AtomicU64,
}

#[derive(Clone)]
pub struct AppState {
    pool: PgPool,
    secret: Arc<Vec<u8>>,
    filters: Filters,
    counters: Arc<Counters>,
}
impl AppState {
    pub fn new(pool: PgPool, secret: String, filters: Filters) -> Result<Self> {
        ensure!(
            !secret.trim().is_empty(),
            "GITHUB_WEBHOOK_SECRET must not be empty"
        );
        Ok(Self {
            pool,
            secret: Arc::new(secret.into_bytes()),
            filters,
            counters: Arc::new(Counters::default()),
        })
    }
}

pub fn valid_signature(secret: &[u8], signature: &str, body: &[u8]) -> bool {
    let Some(signature) = signature
        .strip_prefix("sha256=")
        .and_then(|value| hex::decode(value).ok())
    else {
        return false;
    };
    let Ok(mut verifier) = Hmac::<Sha256>::new_from_slice(secret) else {
        return false;
    };
    verifier.update(body);
    verifier.verify_slice(&signature).is_ok()
}

pub fn webhook_router(state: AppState) -> Router {
    Router::new()
        .route("/webhook", post(webhook))
        .layer(DefaultBodyLimit::max(MAX_PAYLOAD_BYTES))
        .with_state(state)
}

pub fn internal_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .with_state(state)
}

pub async fn serve(
    state: AppState,
    webhook_bind: SocketAddr,
    internal_bind: SocketAddr,
) -> Result<()> {
    let webhook_listener = tokio::net::TcpListener::bind(webhook_bind).await?;
    let internal_listener = tokio::net::TcpListener::bind(internal_bind).await?;
    tracing::info!(%webhook_bind, %internal_bind, "listeners ready");
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(false);
    async fn shutdown(mut receiver: tokio::sync::watch::Receiver<bool>) {
        let _ = receiver.wait_for(|shutdown| *shutdown).await;
    }
    let signal = async move {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! { result = tokio::signal::ctrl_c() => result?, _ = terminate.recv() => {} }
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await?;
        let _ = shutdown_sender.send(true);
        Ok::<(), std::io::Error>(())
    };
    let webhook_server = axum::serve(webhook_listener, webhook_router(state.clone()))
        .with_graceful_shutdown(shutdown(shutdown_receiver.clone()));
    let internal_server = axum::serve(internal_listener, internal_router(state))
        .with_graceful_shutdown(shutdown(shutdown_receiver));
    tokio::try_join!(
        async { webhook_server.await },
        async { internal_server.await },
        signal
    )?;
    Ok(())
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    (values.next().is_none() && !value.is_empty() && value.len() <= MAX_HEADER_BYTES)
        .then_some(value)
}

async fn webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let signature = header_value(&headers, "x-hub-signature-256").unwrap_or_default();
    if !valid_signature(&state.secret, signature, &body) {
        state.counters.rejected.fetch_add(1, Ordering::Relaxed);
        return (StatusCode::UNAUTHORIZED, "invalid signature");
    }
    let (Some(delivery), Some(event)) = (
        header_value(&headers, "x-github-delivery"),
        header_value(&headers, "x-github-event"),
    ) else {
        return (
            StatusCode::BAD_REQUEST,
            "missing or invalid delivery headers",
        );
    };
    let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return (StatusCode::BAD_REQUEST, "invalid JSON");
    };
    if !state.filters.accepts(event, &payload) {
        state.counters.filtered.fetch_add(1, Ordering::Relaxed);
        return (StatusCode::OK, "filtered");
    }
    match store::ingest(&state.pool, Source::Webhook, delivery, event, &payload).await {
        Ok(Outcome::Duplicate) => {
            state.counters.duplicate.fetch_add(1, Ordering::Relaxed);
            (StatusCode::OK, "duplicate")
        }
        Ok(outcome) => {
            state.counters.accepted.fetch_add(1, Ordering::Relaxed);
            if outcome == Outcome::Invalid {
                tracing::warn!("delivery retained with invalid projection");
            }
            (StatusCode::ACCEPTED, outcome.as_str())
        }
        Err(_) => {
            state.counters.failed.fetch_add(1, Ordering::Relaxed);
            tracing::error!("delivery transaction failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "persistence failed; redelivery required",
            )
        }
    }
}

async fn ready(State(state): State<AppState>) -> StatusCode {
    match sqlx::query("SELECT 1 FROM gha_deliveries LIMIT 0")
        .execute(&state.pool)
        .await
    {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let mut output = String::from(
        "# HELP gha_observer_webhook_requests_total Webhook requests processed by this process.\n# TYPE gha_observer_webhook_requests_total counter\n",
    );
    for (outcome, counter) in [
        ("accepted", &state.counters.accepted),
        ("duplicate", &state.counters.duplicate),
        ("filtered", &state.counters.filtered),
        ("rejected", &state.counters.rejected),
        ("failed", &state.counters.failed),
    ] {
        output.push_str(&format!(
            "gha_observer_webhook_requests_total{{outcome=\"{outcome}\"}} {}\n",
            counter.load(Ordering::Relaxed)
        ));
    }
    let active_jobs =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gha_jobs WHERE status <> 'completed'")
            .fetch_one(&state.pool)
            .await;
    let completed_jobs = sqlx::query_as::<_, (String, i64)>(
        "SELECT CASE WHEN conclusion IN ('success','failure','cancelled','skipped','timed_out','action_required','neutral','stale','startup_failure') THEN conclusion ELSE 'other' END AS conclusion, count(*) FROM gha_jobs WHERE status='completed' GROUP BY 1"
    ).fetch_all(&state.pool).await;
    output.push_str("# HELP gha_observer_database_up Whether the metrics database queries succeeded.\n# TYPE gha_observer_database_up gauge\n");
    output.push_str(&format!(
        "gha_observer_database_up {}\n",
        i32::from(active_jobs.is_ok() && completed_jobs.is_ok())
    ));
    if let Ok(count) = active_jobs {
        output.push_str("# HELP gha_observer_active_jobs Observed jobs without terminal state.\n# TYPE gha_observer_active_jobs gauge\n");
        output.push_str(&format!("gha_observer_active_jobs {count}\n"));
    }
    if let Ok(counts) = completed_jobs {
        output.push_str("# HELP gha_observer_completed_jobs Retained completed jobs, including imports; exact event-time history is available through SQL.\n# TYPE gha_observer_completed_jobs gauge\n");
        for conclusion in [
            "success",
            "failure",
            "cancelled",
            "skipped",
            "timed_out",
            "action_required",
            "neutral",
            "stale",
            "startup_failure",
            "other",
        ] {
            let count = counts
                .iter()
                .find(|(key, _)| key == conclusion)
                .map_or(0, |(_, count)| *count);
            output.push_str(&format!(
                "gha_observer_completed_jobs{{conclusion=\"{conclusion}\"}} {count}\n"
            ));
        }
    }
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signatures_require_exact_body_and_sha256() {
        let mut signer = Hmac::<Sha256>::new_from_slice(b"test-secret").unwrap();
        signer.update(b"synthetic body");
        let signature = format!("sha256={}", hex::encode(signer.finalize().into_bytes()));
        assert!(valid_signature(
            b"test-secret",
            &signature,
            b"synthetic body"
        ));
        assert!(!valid_signature(
            b"test-secret",
            &signature,
            b"changed body"
        ));
        assert!(!valid_signature(
            b"other-secret",
            &signature,
            b"synthetic body"
        ));
        assert!(!valid_signature(
            b"test-secret",
            "sha1=abc",
            b"synthetic body"
        ));
    }
}
