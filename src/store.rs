use crate::model::{Job, Repository, WorkflowRun, status_rank};
use anyhow::{Result, ensure};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Clone, Copy)]
pub enum Source {
    Webhook,
    Backfill,
    Replay,
    Reconcile,
}
impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Webhook => "webhook",
            Self::Backfill => "backfill",
            Self::Replay => "replay",
            Self::Reconcile => "reconcile",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Projected,
    Unhandled,
    Invalid,
    Duplicate,
}
impl Outcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Projected => "projected",
            Self::Unhandled => "unhandled",
            Self::Invalid => "invalid",
            Self::Duplicate => "duplicate",
        }
    }
}

enum Projection {
    Run(Repository, WorkflowRun),
    Job(Repository, Job),
    Unhandled,
    Invalid,
}

fn projection(event: &str, payload: &Value) -> Projection {
    fn parse<T: serde::de::DeserializeOwned>(payload: &Value, key: &str) -> Option<T> {
        serde_json::from_value(payload.get(key)?.clone()).ok()
    }
    match event {
        "workflow_run" => match (parse(payload, "repository"), parse(payload, "workflow_run")) {
            (Some(repository), Some(run)) => Projection::Run(repository, run),
            _ => Projection::Invalid,
        },
        "workflow_job" => match (parse(payload, "repository"), parse(payload, "workflow_job")) {
            (Some(repository), Some(job)) => Projection::Job(repository, job),
            _ => Projection::Invalid,
        },
        _ => Projection::Unhandled,
    }
}

/// Commit the raw delivery and its projection atomically before acknowledging it.
/// Unsupported and structurally invalid payloads remain available for later analysis.
pub async fn ingest(
    pool: &PgPool,
    source: Source,
    delivery_id: &str,
    event: &str,
    payload: &Value,
) -> Result<Outcome> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SET LOCAL transaction_timeout = '5s'")
        .execute(&mut *transaction)
        .await?;
    let outcome =
        ingest_in_transaction(&mut transaction, source, delivery_id, event, payload).await?;
    transaction.commit().await?;
    Ok(outcome)
}

/// Persist a bounded repair batch with one durable commit, preserving delivery idempotence.
pub async fn ingest_batch(
    pool: &PgPool,
    source: Source,
    deliveries: &[(String, String, Value)],
) -> Result<Vec<Outcome>> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SET LOCAL transaction_timeout = '5s'")
        .execute(&mut *transaction)
        .await?;
    let mut outcomes = Vec::with_capacity(deliveries.len());
    for (delivery_id, event, payload) in deliveries {
        outcomes.push(
            ingest_in_transaction(&mut transaction, source, delivery_id, event, payload).await?,
        );
    }
    transaction.commit().await?;
    Ok(outcomes)
}

async fn ingest_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    source: Source,
    delivery_id: &str,
    event: &str,
    payload: &Value,
) -> Result<Outcome> {
    let projection = projection(event, payload);
    let outcome = match &projection {
        Projection::Run(..) | Projection::Job(..) => Outcome::Projected,
        Projection::Unhandled => Outcome::Unhandled,
        Projection::Invalid => Outcome::Invalid,
    };
    let inserted = sqlx::query("INSERT INTO gha_deliveries (source,delivery_id,event,action,repository,payload,projection_status) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING")
        .bind(source.as_str()).bind(delivery_id).bind(event)
        .bind(payload.get("action").and_then(Value::as_str))
        .bind(payload.pointer("/repository/full_name").and_then(Value::as_str).map(str::to_ascii_lowercase))
        .bind(payload).bind(outcome.as_str()).execute(&mut **transaction).await?.rows_affected();
    if inserted == 0 {
        let matches: bool = sqlx::query_scalar(
            "SELECT event=$3 AND payload=$4 FROM gha_deliveries WHERE source=$1 AND delivery_id=$2",
        )
        .bind(source.as_str())
        .bind(delivery_id)
        .bind(event)
        .bind(payload)
        .fetch_one(&mut **transaction)
        .await?;
        ensure!(
            matches,
            "delivery identifier reused with a different event or payload"
        );
        return Ok(Outcome::Duplicate);
    }
    match projection {
        Projection::Run(repository, run) => project_run(transaction, &repository, &run).await?,
        Projection::Job(repository, job) => project_job(transaction, &repository, &job).await?,
        Projection::Unhandled | Projection::Invalid => {}
    }
    Ok(outcome)
}

async fn project_run(
    transaction: &mut Transaction<'_, Postgres>,
    repository: &Repository,
    run: &WorkflowRun,
) -> Result<()> {
    sqlx::query("INSERT INTO gha_workflow_runs (repository_id,repository,run_id,run_attempt,workflow_name,branch,event,status,status_rank,conclusion,created_at,updated_at,started_at,html_url) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (repository_id,run_id,run_attempt) DO UPDATE SET repository=excluded.repository,workflow_name=excluded.workflow_name,branch=excluded.branch,event=excluded.event,status=excluded.status,status_rank=excluded.status_rank,conclusion=excluded.conclusion,updated_at=excluded.updated_at,started_at=COALESCE(excluded.started_at,gha_workflow_runs.started_at),html_url=excluded.html_url
        WHERE excluded.status_rank >= gha_workflow_runs.status_rank AND excluded.updated_at >= gha_workflow_runs.updated_at")
        .bind(repository.id.0).bind(repository.full_name.to_ascii_lowercase()).bind(run.id.0).bind(run.run_attempt).bind(&run.name).bind(&run.head_branch).bind(&run.event).bind(&run.status).bind(status_rank(&run.status)).bind(&run.conclusion).bind(run.created_at).bind(run.updated_at).bind(run.run_started_at).bind(&run.html_url)
        .execute(&mut **transaction).await?;
    Ok(())
}

async fn project_job(
    transaction: &mut Transaction<'_, Postgres>,
    repository: &Repository,
    job: &Job,
) -> Result<()> {
    let changed = sqlx::query("INSERT INTO gha_jobs (repository_id,repository,job_id,run_id,run_attempt,job_name,workflow_name,branch,status,status_rank,conclusion,created_at,started_at,completed_at,html_url,runner_name,runner_group_name,runner_labels) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        ON CONFLICT (repository_id,job_id) DO UPDATE SET repository=excluded.repository,job_name=excluded.job_name,workflow_name=COALESCE(excluded.workflow_name,gha_jobs.workflow_name),branch=COALESCE(excluded.branch,gha_jobs.branch),status=excluded.status,status_rank=excluded.status_rank,conclusion=excluded.conclusion,created_at=COALESCE(excluded.created_at,gha_jobs.created_at),started_at=COALESCE(excluded.started_at,gha_jobs.started_at),completed_at=COALESCE(excluded.completed_at,gha_jobs.completed_at),html_url=excluded.html_url,runner_name=COALESCE(excluded.runner_name,gha_jobs.runner_name),runner_group_name=COALESCE(excluded.runner_group_name,gha_jobs.runner_group_name),runner_labels=excluded.runner_labels
        WHERE excluded.status_rank >= gha_jobs.status_rank AND (gha_jobs.completed_at IS NULL OR excluded.completed_at >= gha_jobs.completed_at)")
        .bind(repository.id.0).bind(repository.full_name.to_ascii_lowercase()).bind(job.id.0).bind(job.run_id.0).bind(job.run_attempt).bind(&job.name).bind(&job.workflow_name).bind(&job.head_branch).bind(&job.status).bind(status_rank(&job.status)).bind(&job.conclusion).bind(job.created_at).bind(job.started_at).bind(job.completed_at).bind(&job.html_url).bind(&job.runner_name).bind(&job.runner_group_name).bind(&job.labels)
        .execute(&mut **transaction).await?.rows_affected();
    if changed == 0 {
        return Ok(());
    }
    for step in &job.steps {
        sqlx::query("INSERT INTO gha_steps (repository_id,job_id,step_number,step_name,status,conclusion,started_at,completed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (repository_id,job_id,step_number) DO UPDATE SET step_name=excluded.step_name,status=excluded.status,conclusion=excluded.conclusion,started_at=COALESCE(excluded.started_at,gha_steps.started_at),completed_at=COALESCE(excluded.completed_at,gha_steps.completed_at)
            WHERE gha_steps.status <> 'completed' OR excluded.status = 'completed'")
            .bind(repository.id.0).bind(job.id.0).bind(step.number).bind(&step.name).bind(&step.status).bind(&step.conclusion).bind(step.started_at).bind(step.completed_at).execute(&mut **transaction).await?;
    }
    Ok(())
}
