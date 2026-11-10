//! Real PostgreSQL readers and separate collector processes. HTTP gates control
//! page boundaries; a test-only trigger gates an actual uncommitted snapshot.
use anyhow::{Context, Result};
use axum::{Json, Router, extract::Query, routing::get};
use github_actions_observer::{
    config::Filters,
    reconcile::{ReconcileOptions, reconcile},
    store,
};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{collections::HashMap, process::Stdio, time::Duration};
use tokio::{
    process::{Child, Command},
    sync::{mpsc, oneshot},
    time::{sleep, timeout},
};

const DEADLINE: Duration = Duration::from_secs(30);
type Request = (String, usize, oneshot::Sender<Value>);

fn run(id: i64, status: &str) -> Value {
    json!({"id":id,"run_attempt":1,"name":"CI","head_branch":"main",
        "event":"push","status":status,"conclusion":null,
        "created_at":"2026-01-01T01:00:00Z","updated_at":"2026-01-01T02:00:00Z",
        "html_url":"https://example.com/run"})
}

async fn pool(schema: &str) -> Result<PgPool> {
    let search_path = format!("SET search_path TO {schema}");
    Ok(PgPoolOptions::new()
        .max_connections(5)
        .after_connect(move |connection, _| {
            let sql = search_path.clone();
            Box::pin(async move {
                sqlx::query(&sql).execute(connection).await?;
                Ok(())
            })
        })
        .connect(&std::env::var("TEST_DATABASE_URL")?)
        .await?)
}

// Run only in an explicitly spawned test process, so killing it closes real
// database connections and discards all in-memory pagination/retry state.
#[tokio::test]
#[ignore = "subprocess entry point for recovery_is_atomic_to_database_readers"]
async fn reconcile_subprocess() -> Result<()> {
    let schema = std::env::var("ATOMICITY_SCHEMA")?;
    assert!(
        schema.starts_with("atomicity_")
            && schema
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
    );
    let pool = pool(&schema).await?;
    reconcile(
        &pool,
        &Filters::new("", "", "", "")?,
        ReconcileOptions {
            repositories: vec!["example/repo".into()],
            token: "synthetic-fixture".into(),
            api_base: std::env::var("ATOMICITY_API")?.parse()?,
            max_requests: std::env::var("ATOMICITY_BUDGET")?.parse()?,
            rate_limit_reserve: 100,
        },
    )
    .await
}

fn collector(schema: &str, api: &str, budget: u64) -> Result<Child> {
    Ok(Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "reconcile_subprocess",
            "--ignored",
            "--nocapture",
        ])
        .env("ATOMICITY_SCHEMA", schema)
        .env("ATOMICITY_API", api)
        .env("ATOMICITY_BUDGET", budget.to_string())
        .env("RUST_LOG", "off")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?)
}

// One statement gives readers a consistent view of headers AND membership. Keep
// IDs and timestamps too: count-only assertions could miss an overwritten row.
async fn snapshots(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT coalesce(jsonb_agg(to_jsonb(s) || jsonb_build_object('runs',
            (SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY run_id), '[]'::jsonb)
             FROM gha_queue_snapshot_runs r WHERE r.snapshot_id=s.id)) ORDER BY s.id), '[]'::jsonb)
         FROM gha_queue_snapshots s",
    )
    .fetch_one(pool)
    .await?)
}

async fn unchanged(pool: &PgPool, previous: &Value) -> Result<()> {
    assert_eq!(&snapshots(pool).await?, previous);
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM gha_workflow_runs), (SELECT count(*) FROM gha_deliveries)",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(counts, (0, 0));
    Ok(())
}

async fn page(
    requests: &mut mpsc::Receiver<Request>,
    status: &str,
    page: usize,
) -> Result<oneshot::Sender<Value>> {
    let (actual_status, actual_page, reply) = timeout(DEADLINE, requests.recv())
        .await?
        .context("HTTP server stopped")?;
    assert_eq!((actual_status.as_str(), actual_page), (status, page));
    Ok(reply)
}

fn respond(reply: oneshot::Sender<Value>, total: usize, runs: Vec<Value>) {
    assert!(
        reply
            .send(json!({"total_count":total,"workflow_runs":runs}))
            .is_ok()
    );
}

async fn earlier_statuses(requests: &mut mpsc::Receiver<Request>) -> Result<()> {
    // The same run transitions between two independently observed statuses.
    for (status, id) in [
        ("queued", 2),
        ("pending", 2),
        ("waiting", 3),
        ("requested", 4),
    ] {
        respond(page(requests, status, 1).await?, 1, vec![run(id, status)]);
    }
    Ok(())
}

async fn full_first_page(requests: &mut mpsc::Receiver<Request>, first_id: i64) -> Result<()> {
    respond(
        page(requests, "in_progress", 1).await?,
        101,
        (first_id..first_id + 100)
            .map(|id| run(id, "in_progress"))
            .collect(),
    );
    Ok(())
}

async fn failed(child: Child, expected: &str) -> Result<()> {
    let output = timeout(DEADLINE, child.wait_with_output()).await??;
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr)?;
    assert!(error.contains(expected), "expected {expected}, got {error}");
    Ok(())
}

async fn receipt(pool: &PgPool, stage: &str) -> Result<()> {
    let state: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object('snapshots',(SELECT count(*) FROM gha_queue_snapshots),
          'queue_rows',(SELECT count(*) FROM gha_queue_snapshot_runs),
          'workflow_runs',(SELECT count(*) FROM gha_workflow_runs),
          'deliveries',(SELECT count(*) FROM gha_deliveries),
          'duplicate_snapshot_runs',(SELECT count(*) FROM
            (SELECT snapshot_id,run_id FROM gha_queue_snapshot_runs GROUP BY 1,2 HAVING count(*)>1) d))",
    ).fetch_one(pool).await?;
    println!("DATABASE_RECEIPT {}", json!({"stage":stage,"state":state}));
    Ok(())
}

#[tokio::test]
async fn recovery_is_atomic_to_database_readers() -> Result<()> {
    if std::env::var("TEST_DATABASE_URL").is_err() {
        eprintln!("SKIPPED: TEST_DATABASE_URL is required");
        return Ok(());
    }
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let schema = format!("atomicity_{suffix}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("TEST_DATABASE_URL")?)
        .await?;
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let reader = pool(&schema).await?;
    store::MIGRATOR.run(&reader).await?;
    let previous_id: i64 = sqlx::query_scalar(
        "INSERT INTO gha_queue_snapshots(repository,checked_at) VALUES('example/repo','2025-01-01') RETURNING id",
    ).fetch_one(&reader).await?;
    sqlx::query("INSERT INTO gha_queue_snapshot_runs(snapshot_id,run_id,run_attempt,status,html_url,created_at)
        VALUES($1,99,1,'queued','https://example.com/old','2025-01-01')")
        .bind(previous_id).execute(&reader).await?;
    let previous = snapshots(&reader).await?;
    let (send, mut requests) = mpsc::channel::<Request>(1);
    let routes = Router::new()
        .route(
            "/rate_limit",
            get(|| async {
                Json(json!({"resources":{"core":{"remaining":5000,"reset":4102444800_u64}}}))
            }),
        )
        .route(
            "/repos/example/repo",
            get(|| async { Json(json!({"id":1,"full_name":"example/repo"})) }),
        )
        .route(
            "/repos/example/repo/actions/runs",
            get(move |Query(q): Query<HashMap<String, String>>| {
                let send = send.clone();
                async move {
                    let (reply, receive) = oneshot::channel();
                    send.send((q["status"].clone(), q["page"].parse().unwrap(), reply))
                        .await
                        .unwrap();
                    // A killed collector drops the reply; no fixture task panics.
                    Json(
                        receive
                            .await
                            .unwrap_or_else(|_| json!({"total_count":0,"workflow_runs":[]})),
                    )
                }
            }),
        )
        .route(
            "/repos/example/repo/actions/runs/{id}/attempts/1/jobs",
            get(|| async { Json(json!({"total_count":0,"jobs":[]})) }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let api = format!("http://{}/", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });

    for scenario in [
        "persistent_second_page",
        "budget_during_retry",
        "process_killed_before_retry",
    ] {
        let mut child = collector(
            &schema,
            &api,
            if scenario == "budget_during_retry" {
                9
            } else {
                500
            },
        )?;
        earlier_statuses(&mut requests).await?;
        full_first_page(&mut requests, 1000).await?;
        let second = page(&mut requests, "in_progress", 2).await?;
        unchanged(&reader, &previous).await?;
        respond(second, 101, vec![]);
        let retry = page(&mut requests, "in_progress", 1).await?;
        unchanged(&reader, &previous).await?;
        if scenario == "process_killed_before_retry" {
            child.kill().await?;
            child.wait().await?;
            drop(retry);
        } else {
            respond(
                retry,
                101,
                (2000..2100).map(|id| run(id, "in_progress")).collect(),
            );
            if scenario == "persistent_second_page" {
                let second = page(&mut requests, "in_progress", 2).await?;
                unchanged(&reader, &previous).await?;
                respond(second, 101, vec![]);
                failed(child, "short page").await?;
            } else {
                failed(child, "request budget reached").await?;
            }
        }
        unchanged(&reader, &previous).await?;
        assert!(requests.try_recv().is_err(), "unexpected additional retry");
        receipt(&reader, scenario).await?;
    }

    // Pause the second membership INSERT, after the header and first member have
    // executed. pg_locks proves the collector reached this precise DB boundary.
    let lock_key = (suffix % 1_000_000_000) as i64;
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION pause_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF NEW.run_id=3 THEN PERFORM pg_advisory_xact_lock({lock_key}); END IF; RETURN NEW; END $$;
         CREATE TRIGGER pause_snapshot BEFORE INSERT ON gha_queue_snapshot_runs
         FOR EACH ROW EXECUTE FUNCTION pause_snapshot();"
    )).execute(&reader).await?;
    let mut blocker = reader.acquire().await?;
    for kill in [true, false] {
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_key)
            .execute(&mut *blocker)
            .await?;
        let mut child = collector(&schema, &api, 500)?;
        earlier_statuses(&mut requests).await?;
        full_first_page(&mut requests, 1000).await?;
        respond(page(&mut requests, "in_progress", 2).await?, 101, vec![]);
        full_first_page(&mut requests, 2000).await?;
        let last = page(&mut requests, "in_progress", 2).await?;
        unchanged(&reader, &previous).await?;
        respond(last, 101, vec![run(2100, "in_progress")]);
        timeout(DEADLINE, async {
            loop {
                let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=0 AND objid=$1::bigint::oid AND NOT granted)")
                    .bind(lock_key).fetch_one(&reader).await?;
                if waiting { break Ok::<_, anyhow::Error>(()); }
                sleep(Duration::from_millis(10)).await;
            }
        }).await??;
        unchanged(&reader, &previous).await?;
        receipt(&reader, "header_and_first_member_uncommitted").await?;
        if kill {
            child.kill().await?;
            child.wait().await?;
        }
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut *blocker)
            .await?;
        if kill {
            unchanged(&reader, &previous).await?;
            receipt(&reader, "process_killed_in_transaction").await?;
        } else {
            let output = timeout(DEADLINE, child.wait_with_output()).await??;
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
    drop(blocker);
    let published = snapshots(&reader).await?;
    assert_eq!(published.as_array().unwrap().len(), 2);
    assert_eq!(published[0], previous[0]);
    let members = published[1]["runs"].as_array().unwrap();
    assert_eq!(
        members
            .iter()
            .map(|r| r["run_id"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert_eq!(members[0]["status"], "pending");
    let expected: Vec<i64> = [2, 3, 4].into_iter().chain(2000..=2100).collect();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT run_id FROM gha_workflow_runs ORDER BY run_id")
            .fetch_all(&reader)
            .await?,
        expected
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gha_deliveries")
            .fetch_one(&reader)
            .await?,
        104
    );
    receipt(&reader, "complete_replacement_committed").await?;

    // A fresh invocation observes the same complete data. It adds an observation,
    // but must not duplicate run projections or idempotent history deliveries.
    let child = collector(&schema, &api, 500)?;
    earlier_statuses(&mut requests).await?;
    full_first_page(&mut requests, 2000).await?;
    respond(
        page(&mut requests, "in_progress", 2).await?,
        101,
        vec![run(2100, "in_progress")],
    );
    let output = timeout(DEADLINE, child.wait_with_output()).await??;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT run_id FROM gha_workflow_runs ORDER BY run_id")
            .fetch_all(&reader)
            .await?,
        expected
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gha_deliveries")
            .fetch_one(&reader)
            .await?,
        104
    );
    let repeated = snapshots(&reader).await?;
    assert_eq!(repeated.as_array().unwrap().len(), 3);
    assert_eq!(
        &repeated.as_array().unwrap()[..2],
        published.as_array().unwrap()
    );
    assert_eq!(
        repeated[2]["runs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["run_id"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    receipt(&reader, "fresh_invocation_no_duplicate_runs").await?;
    server.abort();
    reader.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    Ok(())
}
