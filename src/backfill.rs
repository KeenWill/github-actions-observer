use crate::{
    config::{Filters, valid_repository},
    store::{self, Source},
};
use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, SecondsFormat, Utc};
use reqwest::{Client, Url};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;

const PAGE_SIZE: usize = 100;
const SEARCH_LIMIT: u64 = 1000;

pub struct BackfillOptions {
    pub repositories: Vec<String>,
    pub created_since: DateTime<Utc>,
    pub created_until: DateTime<Utc>,
    pub token: String,
    pub api_base: Url,
}

struct GitHub {
    client: Client,
    base: Url,
    token: String,
}
impl GitHub {
    async fn get(&self, path: &str, parameters: &[(&str, String)]) -> Result<Value> {
        let url = self.base.join(path)?;
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.token)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .query(parameters)
            .send()
            .await
            .context("GitHub request failed")?;
        ensure!(
            response.status().is_success(),
            "GitHub returned {}; import is incomplete and safe to rerun",
            response.status()
        );
        response
            .json()
            .await
            .context("invalid GitHub JSON response")
    }
    async fn pages(
        &self,
        path: &str,
        key: &str,
        parameters: &[(&str, String)],
        limit: Option<u64>,
    ) -> Result<Vec<Value>> {
        let mut all = Vec::new();
        let mut identifiers = std::collections::HashSet::new();
        let mut expected_total = None;
        let mut page = 1_u64;
        loop {
            let mut parameters = parameters.to_vec();
            parameters.extend([
                ("per_page", PAGE_SIZE.to_string()),
                ("page", page.to_string()),
            ]);
            let response = self.get(path, &parameters).await?;
            let total = response["total_count"]
                .as_u64()
                .context("missing total_count")?;
            if let Some(limit) = limit {
                ensure!(
                    total <= limit,
                    "creation window contains {total} runs, exceeding GitHub's {limit}-result limit; split the window and rerun"
                );
            }
            ensure!(
                expected_total.is_none_or(|expected| expected == total),
                "GitHub result count changed during pagination; rerun import"
            );
            expected_total = Some(total);
            let items = response[key]
                .as_array()
                .context("missing paginated result list")?;
            for item in items {
                let identifier = item["id"].as_i64().context("paginated item missing id")?;
                ensure!(
                    identifiers.insert(identifier),
                    "duplicate item across GitHub pages; results changed, rerun import"
                );
                all.push(item.clone());
            }
            ensure!(
                all.len() as u64 <= total,
                "GitHub returned more results than total_count; rerun import"
            );
            if all.len() as u64 == total {
                return Ok(all);
            }
            ensure!(
                items.len() == PAGE_SIZE,
                "GitHub returned a short page before total_count was reached; import incomplete"
            );
            page += 1;
        }
    }
}

pub async fn backfill(pool: &PgPool, filters: &Filters, options: BackfillOptions) -> Result<()> {
    ensure!(
        !options.repositories.is_empty(),
        "at least one explicit repository is required"
    );
    ensure!(
        options
            .repositories
            .iter()
            .all(|repository| valid_repository(repository)),
        "repositories must be owner/repository"
    );
    ensure!(
        options.created_since < options.created_until,
        "created-since must be before created-until"
    );
    ensure!(
        options.created_since.timestamp_subsec_nanos() == 0
            && options.created_until.timestamp_subsec_nanos() == 0,
        "creation bounds must use whole seconds"
    );
    ensure!(
        options.api_base.scheme() == "https",
        "GitHub API base must use HTTPS"
    );
    ensure!(
        options.api_base.username().is_empty()
            && options.api_base.password().is_none()
            && options.api_base.query().is_none()
            && options.api_base.fragment().is_none(),
        "GitHub API base must not contain credentials, query or fragment"
    );
    ensure!(
        options.api_base.path().ends_with('/'),
        "GitHub API base must end with /"
    );
    ensure!(
        !options.token.trim().is_empty(),
        "GITHUB_TOKEN must not be empty"
    );
    let github = GitHub {
        client: Client::builder()
            .user_agent("github-actions-observer")
            .timeout(std::time::Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        base: options.api_base,
        token: options.token,
    };
    let created = format!(
        "{}..{}",
        options
            .created_since
            .to_rfc3339_opts(SecondsFormat::Secs, true),
        options
            .created_until
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    );
    for repository_name in options.repositories {
        let repository = github.get(&format!("repos/{repository_name}"), &[]).await?;
        let runs = github
            .pages(
                &format!("repos/{repository_name}/actions/runs"),
                "workflow_runs",
                &[("created", created.clone())],
                Some(SEARCH_LIMIT),
            )
            .await?;
        for run in runs {
            let run_id = run["id"].as_i64().context("run missing id")?;
            let attempt_count = run["run_attempt"]
                .as_i64()
                .context("run missing run_attempt")?;
            ensure!(attempt_count > 0, "run_attempt must be positive");
            for attempt in 1..=attempt_count {
                let historical_run = github
                    .get(
                        &format!(
                            "repos/{repository_name}/actions/runs/{run_id}/attempts/{attempt}"
                        ),
                        &[],
                    )
                    .await?;
                import(pool, filters, "workflow_run", json!({"action":"backfill", "repository":repository, "workflow_run":historical_run})).await?;
                import_jobs(
                    pool,
                    filters,
                    &github,
                    &repository_name,
                    &repository,
                    run_id,
                    attempt,
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn import_jobs(
    pool: &PgPool,
    filters: &Filters,
    github: &GitHub,
    repository_name: &str,
    repository: &Value,
    run_id: i64,
    attempt: i64,
) -> Result<()> {
    let jobs = github
        .pages(
            &format!("repos/{repository_name}/actions/runs/{run_id}/attempts/{attempt}/jobs"),
            "jobs",
            &[],
            None,
        )
        .await?;
    for job in jobs {
        let mut job = job;
        job.as_object_mut()
            .context("job is not an object")?
            .insert("run_attempt".into(), Value::from(attempt));
        import(
            pool,
            filters,
            "workflow_job",
            json!({"action":"backfill", "repository":repository, "workflow_job":job}),
        )
        .await?;
    }
    Ok(())
}

async fn import(pool: &PgPool, filters: &Filters, event: &str, payload: Value) -> Result<()> {
    if !filters.accepts(event, &payload) {
        return Ok(());
    }
    let delivery = format!(
        "{event}:{}",
        hex::encode(Sha256::digest(serde_json::to_vec(&payload)?))
    );
    let outcome = store::ingest(pool, Source::Backfill, &delivery, event, &payload).await?;
    if outcome == store::Outcome::Invalid {
        bail!("backfill payload retained but could not be projected; import incomplete");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, extract::Query, routing::get};
    use std::collections::HashMap;

    #[tokio::test]
    async fn pagination_collects_all_pages_and_rejects_truncation() -> Result<()> {
        let router = Router::new()
            .route(
                "/complete",
                get(|Query(query): Query<HashMap<String, String>>| async move {
                    let page = query
                        .get("page")
                        .and_then(|page| page.parse::<usize>().ok())
                        .unwrap_or(1);
                    let jobs: Vec<_> = if page == 1 {
                        (0..100).map(|id| json!({"id":id})).collect()
                    } else {
                        vec![json!({"id":100})]
                    };
                    Json(json!({"total_count":101,"jobs":jobs}))
                }),
            )
            .route(
                "/short",
                get(|| async { Json(json!({"total_count":101,"jobs":[{"id":1}]})) }),
            )
            .route(
                "/over-limit",
                get(|| async { Json(json!({"total_count":1001,"jobs":[]})) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let github = GitHub {
            client: Client::new(),
            base: Url::parse(&format!("http://{address}/"))?,
            token: "synthetic-test-token".into(),
        };
        assert_eq!(
            github.pages("complete", "jobs", &[], None).await?.len(),
            101
        );
        assert!(github.pages("short", "jobs", &[], None).await.is_err());
        assert!(
            github
                .pages("over-limit", "jobs", &[], Some(SEARCH_LIMIT))
                .await
                .is_err()
        );
        server.abort();
        Ok(())
    }
}
