//! Optional in-cluster ARC runner/pod mapping. GitHub-hosted runners remain unmapped.
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::PgPool;
use std::time::Duration;

pub async fn collect(pool: &PgPool) -> Result<()> {
    let directory = "/var/run/secrets/kubernetes.io/serviceaccount";
    let token = tokio::fs::read_to_string(format!("{directory}/token")).await?;
    let ca = tokio::fs::read(format!("{directory}/ca.crt")).await?;
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&ca)?)
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut continuation = String::new();
    loop {
        let response = client
            .get("https://kubernetes.default.svc/api/v1/pods")
            .bearer_auth(token.trim())
            .query(&[
                ("labelSelector", "actions.github.com/scale-set-name"),
                ("limit", "500"),
                ("continue", continuation.as_str()),
            ])
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "Kubernetes pod lookup returned {}",
            response.status()
        );
        let payload: Value = response.json().await?;
        let items = payload["items"]
            .as_array()
            .context("Kubernetes pod list missing items")?;
        for pod in items {
            let Some(node) = pod
                .pointer("/spec/nodeName")
                .and_then(Value::as_str)
                .filter(|n| !n.is_empty())
            else {
                continue;
            };
            let field = |key: &str| {
                pod.pointer(key)
                    .and_then(Value::as_str)
                    .with_context(|| format!("pod missing {key}"))
            };
            let created =
                chrono::DateTime::parse_from_rfc3339(field("/metadata/creationTimestamp")?)?;
            sqlx::query("INSERT INTO gha_runner_hosts(runner_name,pod_uid,namespace,worker_node,pod_created_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(runner_name,pod_uid) DO UPDATE SET last_seen_at=now()")
                .bind(field("/metadata/name")?).bind(field("/metadata/uid")?).bind(field("/metadata/namespace")?).bind(node).bind(created).execute(pool).await?;
        }
        continuation = payload
            .pointer("/metadata/continue")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into();
        if continuation.is_empty() {
            break;
        }
    }
    Ok(())
}
