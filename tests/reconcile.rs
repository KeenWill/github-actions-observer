use anyhow::Result;
use axum::{Json, Router, extract::Query, routing::get};
use github_actions_observer::{
    config::Filters,
    reconcile::{ReconcileOptions, reconcile},
    store::{self, Source},
};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

fn run(id: i64, status: &str) -> Value {
    json!({"id":id,"run_attempt":1,"name":"CI","head_branch":"main","event":"push","status":status,"conclusion":if status=="completed" {Some("cancelled")}else{None},"created_at":"2026-01-01T01:00:00Z","updated_at":"2026-01-01T02:00:00Z","html_url":"https://example.com/run","extra_future_field":{"preserve":true}})
}
#[tokio::test]
async fn repairs_missing_cancellation_discovers_queue_and_preserves_payloads() -> Result<()> {
    let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!("SKIPPED: TEST_DATABASE_URL is required");
        return Ok(());
    };
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    let schema = format!(
        "reconcile_test_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let search_path = format!("SET search_path TO {schema}");
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .after_connect(move |c, _| {
            let s = search_path.clone();
            Box::pin(async move {
                sqlx::query(&s).execute(c).await?;
                Ok(())
            })
        })
        .connect(&url)
        .await?;
    store::MIGRATOR.run(&pool).await?;
    let repository = json!({"id":1,"full_name":"example/repo"});
    store::ingest(
        &pool,
        Source::Webhook,
        "queued",
        "workflow_run",
        &json!({"repository":repository,"workflow_run":run(1,"queued")}),
    )
    .await?;
    let routes=Router::new()
        .route("/rate_limit",get(||async {Json(json!({"resources":{"core":{"remaining":5000,"reset":4102444800_u64}}}))}))
        .route("/repos/example/repo",get(||async{Json(json!({"id":1,"full_name":"example/repo"}))}))
        .route("/repos/example/repo/actions/runs",get(|Query(q):Query<HashMap<String,String>>|async move{let runs=if q["status"]=="queued" {vec![run(2,"queued")]}else{vec![]};Json(json!({"total_count":runs.len(),"workflow_runs":runs}))}))
        .route("/repos/example/repo/actions/runs/1",get(||async{Json(run(1,"completed"))}))
        .route("/repos/example/repo/actions/runs/1/attempts/1/jobs",get(||async{Json(json!({"total_count":1,"jobs":[{"id":11,"run_id":1,"name":"test","status":"completed","conclusion":"cancelled","completed_at":"2026-01-01T02:00:00Z","html_url":"https://example.com/job","runner_name":"runner-pod","labels":["self-hosted"],"unmodeled":"retained"}]}))}))
        .route("/repos/example/repo/actions/runs/2/attempts/1/jobs",get(||async{Json(json!({"total_count":0,"jobs":[]}))}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let api_base = reqwest::Url::parse(&format!("http://{}/", listener.local_addr()?))?;
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let options = |max_requests| ReconcileOptions {
        repositories: vec![],
        token: "synthetic".into(),
        api_base: api_base.clone(),
        max_requests,
        rate_limit_reserve: 100,
    };
    let filters = Filters::new("", "", "", "")?;
    // An interrupted listing must not publish a zero or partial queue.
    assert!(reconcile(&pool, &filters, options(3)).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gha_queue_snapshots")
            .fetch_one(&pool)
            .await?,
        0
    );
    reconcile(&pool, &filters, options(50)).await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT conclusion FROM gha_workflow_runs WHERE run_id=1")
            .fetch_one(&pool)
            .await?,
        "cancelled"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT run_id FROM gha_queue_snapshot_runs")
            .fetch_one(&pool)
            .await?,
        2
    );
    assert_eq!(sqlx::query_scalar::<_,String>("SELECT payload->'workflow_job'->>'unmodeled' FROM gha_deliveries WHERE source='reconcile' AND event='workflow_job'").fetch_one(&pool).await?,"retained");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gha_deliveries WHERE source='webhook'")
            .fetch_one(&pool)
            .await?,
        1
    );
    reconcile(&pool, &filters, options(50)).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM gha_deliveries WHERE source='reconcile' AND event='workflow_job'"
        )
        .fetch_one(&pool)
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gha_queue_snapshots")
            .fetch_one(&pool)
            .await?,
        2
    );
    // A delivery-ID conflict rolls back the entire batch, including preceding writes.
    let bad_batch = vec![
        (
            "batch-new".into(),
            "workflow_run".into(),
            json!({"repository":repository,"workflow_run":run(3,"queued")}),
        ),
        (
            "queued".into(),
            "workflow_run".into(),
            json!({"repository":repository,"workflow_run":run(999,"queued")}),
        ),
    ];
    assert!(
        store::ingest_batch(&pool, Source::Webhook, &bad_batch)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM gha_deliveries WHERE delivery_id='batch-new'"
        )
        .fetch_one(&pool)
        .await?,
        0
    );
    server.abort();
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    Ok(())
}
