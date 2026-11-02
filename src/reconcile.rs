//! Fresh API reconciliation complements webhook delivery; snapshots never imply past coverage.
use crate::{
    backfill::Pages,
    backfill_http::GitHub,
    config::{Filters, valid_repository},
    model::WorkflowRun,
    store::{self, Source},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::collections::{BTreeMap, BTreeSet};

const ACTIVE_STATUSES: [&str; 5] = ["queued", "pending", "waiting", "requested", "in_progress"];

pub struct ReconcileOptions {
    pub repositories: Vec<String>,
    pub token: String,
    pub api_base: reqwest::Url,
    pub max_requests: u64,
    pub rate_limit_reserve: u64,
}

const REPAIR_BATCH_SIZE: usize = 16;

async fn import_batch(
    pool: &PgPool,
    filters: &Filters,
    payloads: Vec<(&str, Value)>,
) -> Result<()> {
    let mut deliveries = Vec::with_capacity(payloads.len());
    for (event, payload) in payloads {
        ensure!(
            filters.accepts(event, &payload),
            "reconciliation event excluded by filters"
        );
        let delivery = format!(
            "{event}:{}",
            hex::encode(Sha256::digest(serde_json::to_vec(&payload)?))
        );
        deliveries.push((delivery, event.to_owned(), payload));
    }
    for batch in deliveries.chunks(REPAIR_BATCH_SIZE) {
        let outcomes = store::ingest_batch(pool, Source::Reconcile, batch).await?;
        ensure!(
            !outcomes.contains(&store::Outcome::Invalid),
            "API payload retained but projection invalid"
        );
    }
    Ok(())
}

async fn refresh_run(
    pool: &PgPool,
    filters: &Filters,
    github: &mut GitHub,
    repository: &Value,
    run: Value,
) -> Result<()> {
    let model: WorkflowRun = serde_json::from_value(run.clone())?;
    let name = repository["full_name"]
        .as_str()
        .context("repository missing name")?;
    // Repair every attempt with unresolved jobs, as well as the latest attempt.
    let mut attempts: BTreeSet<i32> = sqlx::query_scalar("SELECT DISTINCT run_attempt FROM gha_jobs WHERE repository_id=$1 AND run_id=$2 AND status <> 'completed' UNION SELECT run_attempt FROM gha_workflow_runs WHERE repository_id=$1 AND run_id=$2 AND status <> 'completed'")
        .bind(repository["id"].as_i64().context("repository missing id")?).bind(model.id.0).fetch_all(pool).await?.into_iter().collect();
    attempts.insert(model.run_attempt);
    for attempt in attempts {
        let attempt_run = if attempt == model.run_attempt {
            run.clone()
        } else {
            github
                .get(
                    &format!(
                        "repos/{name}/actions/runs/{}/attempts/{attempt}",
                        model.id.0
                    ),
                    &[],
                )
                .await?
        };
        let Pages::Complete(jobs) = github
            .pages(
                &format!(
                    "repos/{name}/actions/runs/{}/attempts/{attempt}/jobs",
                    model.id.0
                ),
                "jobs",
                &[],
                None,
            )
            .await?
        else {
            anyhow::bail!("job pagination incomplete")
        };
        let mut payloads = Vec::with_capacity(jobs.len() + 1);
        for mut job in jobs {
            job.as_object_mut()
                .context("job must be an object")?
                .insert("run_attempt".into(), Value::from(attempt));
            payloads.push((
                "workflow_job",
                json!({"action":"reconcile","repository":repository,"workflow_job":job}),
            ));
        }
        payloads.push((
            "workflow_run",
            json!({"action":"reconcile","repository":repository,"workflow_run":attempt_run}),
        ));
        import_batch(pool, filters, payloads).await?;
    }

    sqlx::query("INSERT INTO gha_reconciled_runs(repository_id,run_id) VALUES($1,$2) ON CONFLICT(repository_id,run_id) DO UPDATE SET checked_at=now()")
        .bind(repository["id"].as_i64().context("repository missing id")?).bind(model.id.0).execute(pool).await?;
    Ok(())
}

pub async fn reconcile(pool: &PgPool, filters: &Filters, options: ReconcileOptions) -> Result<()> {
    let mut github = GitHub::new(
        options.api_base,
        options.token,
        None,
        options.max_requests,
        options.rate_limit_reserve,
    )
    .await?;
    let mut names: BTreeSet<String> = sqlx::query_scalar("SELECT DISTINCT repository FROM gha_workflow_runs UNION SELECT DISTINCT repository FROM gha_jobs").fetch_all(pool).await?.into_iter().collect();
    names.extend(
        options
            .repositories
            .into_iter()
            .map(|s| s.to_ascii_lowercase()),
    );
    let result = async {
        let mut repositories = Vec::new();
        // Snapshot all repositories first, so a large repair backlog cannot starve the live queue.
        for name in names {
            ensure!(valid_repository(&name), "invalid repository name");
            if !filters.accepts("workflow_run", &json!({"action":"reconcile","repository":{"full_name":name}})) { continue; }
            let repository = github.get(&format!("repos/{name}"), &[]).await?;
            let mut active = BTreeMap::new();
            let observed_at = chrono::Utc::now();
            for status in ACTIVE_STATUSES {
                let Pages::Complete(runs) = github.pages(&format!("repos/{name}/actions/runs"), "workflow_runs", &[("status",status.into())], Some(1000)).await? else { anyhow::bail!("active-run search limit reached; no snapshot written for {name}") };
                for run in runs {
                    let model: WorkflowRun = serde_json::from_value(run.clone())?;
                    active.insert(model.id.0, run);
                }
            }
            let mut tx=pool.begin().await?;
            let snapshot_id: i64=sqlx::query_scalar("INSERT INTO gha_queue_snapshots(repository,checked_at) VALUES($1,$2) RETURNING id").bind(&name).bind(observed_at).fetch_one(&mut *tx).await?;
            for run in active.values() {
                let model: WorkflowRun=serde_json::from_value(run.clone())?;
                if !ACTIVE_STATUSES[..4].contains(&model.status.as_str()) { continue; }
                sqlx::query("INSERT INTO gha_queue_snapshot_runs(snapshot_id,run_id,run_attempt,workflow_name,branch,status,html_url,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
                    .bind(snapshot_id).bind(model.id.0).bind(model.run_attempt).bind(model.name).bind(model.head_branch).bind(model.status).bind(model.html_url).bind(model.created_at).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            tracing::info!(repository=%name, active=active.len(), "API queue snapshot recorded");
            repositories.push((name,repository,active));
        }
        for (name, repository, active) in repositories {
            // Oldest verification first permits bounded invocations to make forward progress.
            let candidates: Vec<i64> = sqlx::query_scalar("WITH unresolved AS (SELECT repository_id,run_id FROM gha_workflow_runs WHERE repository=$1 AND status <> 'completed' UNION SELECT repository_id,run_id FROM gha_jobs WHERE repository=$1 AND status <> 'completed') SELECT u.run_id FROM unresolved u LEFT JOIN gha_reconciled_runs c USING(repository_id,run_id) ORDER BY c.checked_at ASC NULLS FIRST,u.run_id")
                .bind(&name).fetch_all(pool).await?;
            let mut seen=BTreeSet::new();
            for id in candidates.into_iter().chain(active.keys().copied()) {
                if !seen.insert(id) {continue;}
                let run=if let Some(run)=active.get(&id) {run.clone()} else {github.get(&format!("repos/{name}/actions/runs/{id}"), &[]).await?};
                refresh_run(pool,filters,&mut github,&repository,run).await?;
            }
        }
        Ok::<(), anyhow::Error>(())
    }.await;
    let report = github.report();
    tracing::info!(%report,"reconciliation invocation finished");
    result.with_context(|| report)
}
