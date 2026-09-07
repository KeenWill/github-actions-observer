use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use github_actions_observer::{
    config::Filters,
    server::{AppState, internal_router, webhook_router},
    store::{self, Outcome, Source},
};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn job(status: &str, attempt: i32, job_id: i64) -> Value {
    let completed = status == "completed";
    json!({"action":status,"repository":{"id":1,"full_name":"example-org/example-repo"},"workflow_job":{
        "id":job_id,"run_id":10,"run_attempt":attempt,"name":"test","workflow_name":"CI","head_branch":"main","status":status,
        "conclusion":if completed { Some("failure") } else { None },
        "created_at":"2026-01-01T01:00:00Z","started_at":if status != "queued" { Some("2026-01-01T01:01:00Z") } else {None},
        "completed_at":if completed {Some("2026-01-01T02:00:00Z")} else {None},
        "html_url":"https://github.com/example-org/example-repo/actions/runs/10",
        "steps":[{"number":1,"name":"synthetic step","status":status,"conclusion":if completed {Some("failure")} else {None},"started_at":null,"completed_at":null}]
    }})
}

/// CI supplies an isolated PostgreSQL service; local runs must explicitly provide a URL.
#[tokio::test]
async fn durable_history_and_listener_boundaries() -> anyhow::Result<()> {
    let Ok(database_url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!(
            "SKIPPED postgres integration: set TEST_DATABASE_URL to an isolated PostgreSQL instance"
        );
        return Ok(());
    };
    let schema = format!(
        "observer_test_{}_{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let search_path = format!("SET search_path TO {schema}");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .after_connect(move |connection, _| {
            let search_path = search_path.clone();
            Box::pin(async move {
                sqlx::query(&search_path).execute(connection).await?;
                Ok(())
            })
        })
        .connect(&database_url)
        .await?;
    store::MIGRATOR.run(&pool).await?;
    let run = |status: &str, attempt: i32| {
        json!({"repository":{"id":1,"full_name":"example-org/example-repo"},"workflow_run":{
            "id":10,"run_attempt":attempt,"name":"CI","head_branch":"main","event":"push","status":status,"conclusion":if status=="completed" {Some("failure")} else {None},
            "created_at":"2026-01-01T01:00:00Z","updated_at":"2026-01-01T02:00:00Z","run_started_at":"2026-01-01T01:01:00Z","html_url":"https://github.com/example-org/example-repo/actions/runs/10"
        }})
    };
    store::ingest(
        &pool,
        Source::Webhook,
        "run-done",
        "workflow_run",
        &run("completed", 1),
    )
    .await?;
    store::ingest(
        &pool,
        Source::Webhook,
        "run-late",
        "workflow_run",
        &run("queued", 1),
    )
    .await?;
    store::ingest(
        &pool,
        Source::Webhook,
        "run-rerun",
        "workflow_run",
        &run("queued", 2),
    )
    .await?;
    let runs: Vec<(i32, String)> =
        sqlx::query_as("SELECT run_attempt,status FROM gha_workflow_runs ORDER BY run_attempt")
            .fetch_all(&pool)
            .await?;
    assert_eq!(runs, vec![(1, "completed".into()), (2, "queued".into())]);
    let completed = job("completed", 1, 100);
    assert_eq!(
        store::ingest(
            &pool,
            Source::Webhook,
            "complete",
            "workflow_job",
            &completed
        )
        .await?,
        Outcome::Projected
    );
    assert_eq!(
        store::ingest(
            &pool,
            Source::Webhook,
            "complete",
            "workflow_job",
            &completed
        )
        .await?,
        Outcome::Duplicate
    );
    assert!(
        store::ingest(
            &pool,
            Source::Webhook,
            "complete",
            "workflow_job",
            &json!({})
        )
        .await
        .is_err()
    );
    store::ingest(
        &pool,
        Source::Webhook,
        "late-queued",
        "workflow_job",
        &job("queued", 1, 100),
    )
    .await?;
    store::ingest(
        &pool,
        Source::Webhook,
        "late-progress",
        "workflow_job",
        &job("in_progress", 1, 100),
    )
    .await?;
    let status: String = sqlx::query_scalar("SELECT status FROM gha_jobs WHERE job_id=100")
        .fetch_one(&pool)
        .await?;
    assert_eq!(status, "completed");
    store::ingest(
        &pool,
        Source::Webhook,
        "rerun",
        "workflow_job",
        &job("queued", 2, 101),
    )
    .await?;
    let attempts: Vec<i32> = sqlx::query_scalar("SELECT run_attempt FROM gha_jobs ORDER BY job_id")
        .fetch_all(&pool)
        .await?;
    assert_eq!(attempts, vec![1, 2]);
    assert_eq!(
        store::ingest(
            &pool,
            Source::Webhook,
            "future",
            "new_event",
            &json!({"hello":"future"})
        )
        .await?,
        Outcome::Unhandled
    );
    assert_eq!(
        store::ingest(
            &pool,
            Source::Webhook,
            "malformed",
            "workflow_job",
            &json!({})
        )
        .await?,
        Outcome::Invalid
    );
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM gha_job_history WHERE completed_at >= '2026-01-01T01:00:00Z' AND completed_at < '2026-01-01T02:00:00Z'").fetch_one(&pool).await?;
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM gha_job_history WHERE completed_at >= '2026-01-01T01:00:00Z' AND completed_at < '2026-01-01T03:00:00Z'").fetch_one(&pool).await?;
    assert_eq!((before, after), (0, 1));
    // A forced SQL failure proves the receipt cannot commit ahead of the projection.
    sqlx::query(
        "ALTER TABLE gha_jobs ADD CONSTRAINT reject_synthetic_failure CHECK (job_id <> 999)",
    )
    .execute(&pool)
    .await?;
    assert!(
        store::ingest(
            &pool,
            Source::Webhook,
            "rollback",
            "workflow_job",
            &job("completed", 1, 999)
        )
        .await
        .is_err()
    );
    let rolled_back: i64 =
        sqlx::query_scalar("SELECT count(*) FROM gha_deliveries WHERE delivery_id='rollback'")
            .fetch_one(&pool)
            .await?;
    assert_eq!(rolled_back, 0);
    let state = AppState::new(pool.clone(), "synthetic-secret".into(), Filters::default())?;
    let router = webhook_router(state.clone());
    let response = router
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let body = br#"{"hello":"future via HTTP"}"#;
    let mut signer = Hmac::<Sha256>::new_from_slice(b"synthetic-secret")?;
    signer.update(body);
    let signature = format!("sha256={}", hex::encode(signer.finalize().into_bytes()));
    let response = router
        .clone()
        .oneshot(
            Request::post("/webhook")
                .header("x-hub-signature-256", signature)
                .header("x-github-delivery", "signed-http")
                .header("x-github-event", "unknown-future-event")
                .body(Body::from(body.to_vec()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let durable: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM gha_deliveries WHERE delivery_id='signed-http')",
    )
    .fetch_one(&pool)
    .await?;
    assert!(durable);
    let response = router
        .oneshot(Request::post("/webhook").body(Body::from("{}"))?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = internal_router(state)
        .oneshot(Request::get("/metrics").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let metrics = String::from_utf8(to_bytes(response.into_body(), 65536).await?.to_vec())?;
    assert!(metrics.contains("gha_observer_database_up 1"));
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    Ok(())
}
