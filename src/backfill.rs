use crate::backfill_http::GitHub;
use crate::{
    config::{Filters, valid_repository},
    store::{self, Source},
};
use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, SecondsFormat, Utc};
use reqwest::Url;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::path::PathBuf;

const PAGE_SIZE: usize = 100;
const SEARCH_LIMIT: u64 = 1000;

pub struct BackfillOptions {
    pub repositories: Vec<String>,
    pub created_since: DateTime<Utc>,
    pub created_until: DateTime<Utc>,
    pub token: String,
    pub api_base: Url,
    pub cache_dir: Option<PathBuf>,
    pub max_requests: u64,
    pub rate_limit_reserve: u64,
}

#[derive(Debug)]
enum Pages {
    Complete(Vec<Value>),
    TooMany(u64),
}

#[derive(Clone, Copy, Debug)]
struct CreatedWindow {
    since: DateTime<Utc>,
    until: DateTime<Utc>,
}
impl CreatedWindow {
    fn parameter(self) -> String {
        format!(
            "{}..{}",
            self.since.to_rfc3339_opts(SecondsFormat::Secs, true),
            self.until.to_rfc3339_opts(SecondsFormat::Secs, true)
        )
    }
    fn split(self) -> Result<(Self, Self)> {
        ensure!(
            self.since < self.until,
            "more than 1000 runs share one creation second; GitHub's search cap prevents complete discovery"
        );
        let midpoint =
            self.since.timestamp() + (self.until.timestamp() - self.since.timestamp()) / 2;
        Ok((
            Self {
                since: self.since,
                until: DateTime::from_timestamp(midpoint, 0).context("invalid midpoint")?,
            },
            Self {
                since: DateTime::from_timestamp(midpoint + 1, 0).context("invalid midpoint")?,
                until: self.until,
            },
        ))
    }
}

enum WindowTask {
    Visit(CreatedWindow),
    CheckSplit {
        start_len: usize,
        expected_total: u64,
    },
}

impl GitHub {
    async fn pages(
        &mut self,
        path: &str,
        key: &str,
        parameters: &[(&str, String)],
        limit: Option<u64>,
    ) -> Result<Pages> {
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
            if limit.is_some_and(|limit| total > limit) {
                return Ok(Pages::TooMany(total));
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
                return Ok(Pages::Complete(all));
            }
            ensure!(
                items.len() == PAGE_SIZE,
                "GitHub returned a short page before total_count was reached; import incomplete"
            );
            page += 1;
        }
    }
    async fn runs_in_window(
        &mut self,
        repository: &str,
        initial: CreatedWindow,
    ) -> Result<Vec<Value>> {
        let mut windows = vec![WindowTask::Visit(initial)];
        let mut runs = Vec::new();
        let mut identifiers = std::collections::HashSet::new();
        while let Some(task) = windows.pop() {
            let window = match task {
                WindowTask::Visit(window) => window,
                WindowTask::CheckSplit {
                    start_len,
                    expected_total,
                } => {
                    let actual_total = (runs.len() - start_len) as u64;
                    ensure!(
                        actual_total == expected_total,
                        "split-window result count changed: expected {expected_total}, collected {actual_total}; import incomplete; restart with a fresh cache directory for a consistent snapshot"
                    );
                    continue;
                }
            };
            match self
                .pages(
                    &format!("repos/{repository}/actions/runs"),
                    "workflow_runs",
                    &[("created", window.parameter())],
                    Some(SEARCH_LIMIT),
                )
                .await?
            {
                Pages::TooMany(total) => {
                    tracing::info!(total, "splitting oversized run-creation window");
                    let (left, right) = window.split()?;
                    windows.extend([
                        WindowTask::CheckSplit {
                            start_len: runs.len(),
                            expected_total: total,
                        },
                        WindowTask::Visit(right),
                        WindowTask::Visit(left),
                    ]);
                }
                Pages::Complete(page) => {
                    for run in page {
                        let identifier = run["id"].as_i64().context("run missing id")?;
                        ensure!(
                            identifiers.insert(identifier),
                            "run repeated across non-overlapping creation windows; import incomplete"
                        );
                        runs.push(run);
                    }
                }
            }
        }
        Ok(runs)
    }

    async fn run_attempt(
        &mut self,
        repository: &str,
        listed: &Value,
        attempt: i64,
    ) -> Result<Value> {
        let latest = listed["run_attempt"]
            .as_i64()
            .context("run missing run_attempt")?;
        ensure!(attempt > 0 && attempt <= latest, "invalid run attempt");
        if attempt == latest {
            return Ok(listed.clone());
        }
        let run_id = listed["id"].as_i64().context("run missing id")?;
        let historical = self
            .get(
                &format!("repos/{repository}/actions/runs/{run_id}/attempts/{attempt}"),
                &[],
            )
            .await?;
        ensure!(
            historical["id"].as_i64() == Some(run_id)
                && historical["run_attempt"].as_i64() == Some(attempt),
            "GitHub returned a different run or attempt than requested"
        );
        Ok(historical)
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
        options.created_since <= options.created_until,
        "created-since must not be after created-until"
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
    let mut github = GitHub::new(
        options.api_base,
        options.token,
        options.cache_dir,
        options.max_requests,
        options.rate_limit_reserve,
    )
    .await?;
    let window = CreatedWindow {
        since: options.created_since,
        until: options.created_until,
    };
    let result: Result<()> = async {
    for repository_name in options.repositories {
        let repository = github.get(&format!("repos/{repository_name}"), &[]).await?;
        let runs = github.runs_in_window(&repository_name,window).await?;
        for run in runs {
            let run_id = run["id"].as_i64().context("run missing id")?;
            let attempt_count = run["run_attempt"]
                .as_i64()
                .context("run missing run_attempt")?;
            ensure!(attempt_count > 0, "run_attempt must be positive");
            for attempt in 1..=attempt_count {
                let historical_run = github.run_attempt(&repository_name, &run, attempt).await?;
                import(pool, filters, "workflow_run", json!({"action":"backfill", "repository":repository, "workflow_run":historical_run})).await?;
                import_jobs(
                    pool,
                    filters,
                    &mut github,
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
    }.await;
    let report = github.report();
    tracing::info!(%report, "backfill invocation finished");
    result.with_context(|| report)
}

async fn import_jobs(
    pool: &PgPool,
    filters: &Filters,
    github: &mut GitHub,
    repository_name: &str,
    repository: &Value,
    run_id: i64,
    attempt: i64,
) -> Result<()> {
    let Pages::Complete(jobs) = github
        .pages(
            &format!("repos/{repository_name}/actions/runs/{run_id}/attempts/{attempt}/jobs"),
            "jobs",
            &[],
            None,
        )
        .await?
    else {
        bail!("unexpected job-list search limit");
    };
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
#[path = "backfill_tests.rs"]
mod tests;
