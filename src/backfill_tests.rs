use super::*;
use crate::backfill_cache::ResponseCache;
use axum::{
    Json, Router,
    extract::{Query, Request},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    routing::get,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

type RequestLog = Arc<Mutex<Vec<String>>>;
struct MockGitHub {
    base: Url,
    requests: RequestLog,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Drop for MockGitHub {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl MockGitHub {
    async fn start(router: Router, remaining: u64) -> Result<Self> {
        let requests: RequestLog = Arc::default();
        let logged = Arc::clone(&requests);
        let router=router.route("/rate_limit",get(move || async move {Json(json!({"resources":{"core":{"remaining":remaining,"reset":4102444800_u64}}}))}))
            .layer(middleware::from_fn(move |request: Request,next:Next| {
                logged.lock().unwrap().push(request.uri().to_string());
                async move { next.run(request).await }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = Url::parse(&format!("http://{}/", listener.local_addr()?))?;
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        Ok(Self {
            base,
            requests,
            server,
        })
    }
    async fn client(&self, cache: Option<PathBuf>, budget: u64, reserve: u64) -> Result<GitHub> {
        GitHub::new(
            self.base.clone(),
            "synthetic-secret-token".into(),
            cache,
            budget,
            reserve,
        )
        .await
    }
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

fn temporary_path(name: &str) -> Result<PathBuf> {
    Ok(std::env::temp_dir().join(format!(
        "observer-{name}-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    )))
}

#[tokio::test]
async fn latest_attempt_reuses_listed_run_and_older_attempt_uses_correct_endpoint() -> Result<()> {
    let server = MockGitHub::start(
        Router::new().route(
            "/repos/example-org/example-repo/actions/runs/1/attempts/1",
            get(|| async { Json(json!({"id":1,"run_attempt":1,"status":"completed"})) }),
        ),
        5000,
    )
    .await?;
    let mut client = server.client(None, 10, 100).await?;
    let listed = json!({"id":1,"run_attempt":2,"status":"queued"});
    assert_eq!(
        client
            .run_attempt("example-org/example-repo", &listed, 2)
            .await?,
        listed
    );
    assert_eq!(server.count(), 0);
    let previous = client
        .run_attempt("example-org/example-repo", &listed, 1)
        .await?;
    assert_eq!(previous["run_attempt"], 1);
    assert_eq!(previous["status"], "completed");
    assert_eq!(server.count(), 2);
    Ok(())
}

#[tokio::test]
async fn pagination_collects_all_pages_and_rejects_truncation() -> Result<()> {
    let server = MockGitHub::start(
        Router::new()
            .route(
                "/complete",
                get(|Query(query): Query<HashMap<String, String>>| async move {
                    let page = query["page"].parse::<usize>().unwrap();
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
            ),
        5000,
    )
    .await?;
    let mut client = server.client(None, 10, 100).await?;
    let Pages::Complete(jobs) = client.pages("complete", "jobs", &[], None).await? else {
        bail!("unexpected limit");
    };
    assert_eq!(jobs.len(), 101);
    assert!(client.pages("short", "jobs", &[], None).await.is_err());
    Ok(())
}

#[tokio::test]
async fn oversized_windows_split_at_seconds_without_overlap_and_keep_exactly_1000() -> Result<()> {
    let server = MockGitHub::start(
        Router::new().route(
            "/repos/example-org/example-repo/actions/runs",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                let created = &query["created"];
                let page = query["page"].parse::<usize>().unwrap();
                let (total, start) = if created == "2026-01-01T00:00:00Z..2026-01-01T00:00:01Z" {
                    (1001, 0)
                } else if created == "2026-01-01T00:00:00Z..2026-01-01T00:00:00Z" {
                    (1000, 0)
                } else if created == "2026-01-01T00:00:01Z..2026-01-01T00:00:01Z" {
                    (1, 1000)
                } else {
                    panic!("unexpected creation window: {created}")
                };
                let runs: Vec<_> = ((page - 1) * 100..(page * 100).min(total))
                    .map(|id| json!({"id":start+id+1}))
                    .collect();
                Json(json!({"total_count":total,"workflow_runs":runs}))
            }),
        ),
        5000,
    )
    .await?;
    let window = CreatedWindow {
        since: "2026-01-01T00:00:00Z".parse()?,
        until: "2026-01-01T00:00:01Z".parse()?,
    };
    let mut client = server.client(None, 20, 100).await?;
    let runs = client
        .runs_in_window("example-org/example-repo", window)
        .await?;
    assert_eq!(runs.len(), 1001);
    assert_eq!(server.count(), 13);
    assert_eq!(runs.last().unwrap()["id"], 1001);
    assert!(
        CreatedWindow {
            since: window.since,
            until: window.since
        }
        .split()
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn cache_resumes_without_gets_or_credentials_and_uses_private_permissions() -> Result<()> {
    let directory = temporary_path("cache")?;
    let server = MockGitHub::start(
        Router::new().route(
            "/items",
            get(|| async { Json(json!({"synthetic":"payload"})) }),
        ),
        5000,
    )
    .await?;
    let mut first = server.client(Some(directory.clone()), 2, 100).await?;
    let body = first.get("items", &[]).await?;
    assert_eq!(first.requests, 2);
    let mut resumed = server.client(Some(directory.clone()), 0, 100).await?;
    assert_eq!(resumed.get("items", &[]).await?, body);
    assert_eq!((resumed.requests, resumed.cache_hits), (0, 1));
    assert_eq!(server.count(), 2);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&directory)?.permissions().mode() & 0o777,
            0o700
        );
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            assert_eq!(entry.metadata()?.permissions().mode() & 0o777, 0o600);
            let contents = std::fs::read_to_string(entry.path())?;
            assert!(!contents.contains("synthetic-secret-token"));
            assert!(!contents.contains("Authorization"));
            assert!(contents.contains("fetched_at"));
        }
    }
    std::fs::remove_dir_all(directory)?;
    Ok(())
}

#[tokio::test]
async fn budget_stops_before_an_extra_get_including_preflight() -> Result<()> {
    let server = MockGitHub::start(
        Router::new().route("/items", get(|| async { Json(json!({})) })),
        5000,
    )
    .await?;
    let mut client = server.client(None, 2, 100).await?;
    client.get("items", &[]).await?;
    let error = client.get("items", &[]).await.unwrap_err().to_string();
    assert!(error.contains("outbound_requests=2/2"));
    assert_eq!(server.count(), 2);
    let mut zero = server.client(None, 0, 100).await?;
    assert!(zero.get("items", &[]).await.is_err());
    assert_eq!(server.count(), 2);
    Ok(())
}

#[tokio::test]
async fn reserve_is_checked_before_first_request_and_updated_from_headers() -> Result<()> {
    let server = MockGitHub::start(
        Router::new().route(
            "/items",
            get(|| async {
                let mut headers = HeaderMap::new();
                headers.insert("x-ratelimit-remaining", "100".parse().unwrap());
                headers.insert("x-ratelimit-reset", "4102444800".parse().unwrap());
                (headers, Json(json!({})))
            }),
        ),
        101,
    )
    .await?;
    let mut client = server.client(None, 10, 100).await?;
    client.get("items", &[]).await?;
    let error = client.get("items", &[]).await.unwrap_err().to_string();
    assert!(error.contains("remaining=100"));
    assert!(error.contains("reset_unix=4102444800"));
    assert_eq!(server.count(), 2);
    let mut at_floor = server.client(None, 10, 101).await?;
    assert!(at_floor.get("items", &[]).await.is_err());
    assert_eq!(server.count(), 3);
    Ok(())
}

#[tokio::test]
async fn secondary_throttling_reports_retry_after_and_never_retries() -> Result<()> {
    let server = MockGitHub::start(
        Router::new().route(
            "/items",
            get(|| async {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", "120")],
                    Json(json!({"message":"synthetic throttle"})),
                )
            }),
        ),
        5000,
    )
    .await?;
    let mut client = server.client(None, 10, 100).await?;
    let error = client.get("items", &[]).await.unwrap_err().to_string();
    assert!(error.contains("Retry-After=120"));
    assert_eq!(server.count(), 2);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn cache_rejects_public_permissions_and_symlinks() -> Result<()> {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let directory = temporary_path("unsafe-cache")?;
    std::fs::create_dir(&directory)?;
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755))?;
    assert!(ResponseCache::open(directory.clone()).await.is_err());
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    let link = temporary_path("cache-link")?;
    symlink(&directory, &link)?;
    assert!(ResponseCache::open(link.clone()).await.is_err());
    std::fs::remove_file(link)?;
    std::fs::remove_dir(directory)?;
    Ok(())
}

#[tokio::test]
async fn nested_split_counts_must_match_each_parent_even_when_root_total_would_match() -> Result<()>
{
    let server = MockGitHub::start(
        Router::new().route(
            "/repos/example-org/example-repo/actions/runs",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                let (total, start) = match query["created"].as_str() {
                    "2026-01-01T00:00:00Z..2026-01-01T00:00:03Z" => (2000, 0),
                    "2026-01-01T00:00:00Z..2026-01-01T00:00:01Z" => (1001, 0),
                    "2026-01-01T00:00:00Z..2026-01-01T00:00:00Z" => (500, 0),
                    "2026-01-01T00:00:01Z..2026-01-01T00:00:01Z" => (500, 500),
                    "2026-01-01T00:00:02Z..2026-01-01T00:00:03Z" => (1000, 1000),
                    other => panic!("unexpected creation window: {other}"),
                };
                let page = query["page"].parse::<usize>().unwrap();
                let runs: Vec<_> = ((page - 1) * 100..(page * 100).min(total))
                    .map(|id| json!({"id":start+id+1}))
                    .collect();
                Json(json!({"total_count":total,"workflow_runs":runs}))
            }),
        ),
        5000,
    )
    .await?;
    let mut client = server.client(None, 40, 100).await?;
    let error = client
        .runs_in_window(
            "example-org/example-repo",
            CreatedWindow {
                since: "2026-01-01T00:00:00Z".parse()?,
                until: "2026-01-01T00:00:03Z".parse()?,
            },
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("expected 1001, collected 1000"));
    Ok(())
}

#[tokio::test]
async fn resumed_cached_parent_must_match_new_child_counts() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let changed = Arc::new(AtomicBool::new(false));
    let server_changed = Arc::clone(&changed);
    let server = MockGitHub::start(
        Router::new().route(
            "/repos/example-org/example-repo/actions/runs",
            get(move |Query(query): Query<HashMap<String, String>>| {
                let changed = Arc::clone(&server_changed);
                async move {
                    let (total, start) = match query["created"].as_str() {
                        "2026-01-01T00:00:00Z..2026-01-01T00:00:01Z" => (
                            if changed.load(Ordering::Relaxed) {
                                1000
                            } else {
                                1001
                            },
                            0,
                        ),
                        "2026-01-01T00:00:00Z..2026-01-01T00:00:00Z" => (500, 0),
                        "2026-01-01T00:00:01Z..2026-01-01T00:00:01Z" => (500, 500),
                        other => panic!("unexpected creation window: {other}"),
                    };
                    let page = query["page"].parse::<usize>().unwrap();
                    let runs: Vec<_> = ((page - 1) * 100..(page * 100).min(total))
                        .map(|id| json!({"id":start+id+1}))
                        .collect();
                    Json(json!({"total_count":total,"workflow_runs":runs}))
                }
            }),
        ),
        5000,
    )
    .await?;
    let directory = temporary_path("split-resume")?;
    let window = CreatedWindow {
        since: "2026-01-01T00:00:00Z".parse()?,
        until: "2026-01-01T00:00:01Z".parse()?,
    };
    let mut first = server.client(Some(directory.clone()), 2, 100).await?;
    assert!(
        first
            .runs_in_window("example-org/example-repo", window)
            .await
            .unwrap_err()
            .to_string()
            .contains("request budget reached")
    );
    assert_eq!(server.count(), 2);
    changed.store(true, Ordering::Relaxed);
    let mut resumed = server.client(Some(directory.clone()), 20, 100).await?;
    let error = resumed
        .runs_in_window("example-org/example-repo", window)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("expected 1001, collected 1000"));
    assert_eq!(resumed.cache_hits, 1);
    std::fs::remove_dir_all(directory)?;
    Ok(())
}
