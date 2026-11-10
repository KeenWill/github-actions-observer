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
async fn short_active_run_page_reports_filter_and_counts_without_payloads() -> Result<()> {
    let server = MockGitHub::start(
        Router::new().route(
            "/repos/example/repo/actions/runs",
            get(|| async {
                Json(json!({"total_count":2,"workflow_runs":[{"id":1,"private_payload":"do-not-log"}]}))
            }),
        ),
        5000,
    )
    .await?;
    let mut client = server.client(None, 10, 100).await?;
    let error = client
        .pages(
            "repos/example/repo/actions/runs",
            "workflow_runs",
            &[("status", "in_progress".into())],
            Some(SEARCH_LIMIT),
        )
        .await
        .unwrap_err()
        .to_string();
    for detail in [
        "short page",
        "repos/example/repo/actions/runs",
        "status",
        "in_progress",
        "page=1",
        "page_items=1",
        "collected=1",
        "total_count=2",
        "per_page=100",
    ] {
        assert!(error.contains(detail), "missing {detail}: {error}");
    }
    assert!(!error.contains("do-not-log"));
    assert!(!error.contains("synthetic-secret-token"));
    assert_eq!(server.count(), 2);
    Ok(())
}

#[tokio::test]
async fn empty_second_page_reports_accumulated_count_and_stops() -> Result<()> {
    let server = MockGitHub::start(
        Router::new().route(
            "/jobs",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                let jobs: Vec<_> = if query["page"] == "1" {
                    (0..100).map(|id| json!({"id":id})).collect()
                } else {
                    vec![]
                };
                Json(json!({"total_count":101,"jobs":jobs}))
            }),
        ),
        5000,
    )
    .await?;
    let mut client = server.client(None, 10, 100).await?;
    let error = client
        .pages("jobs", "jobs", &[], None)
        .await
        .unwrap_err()
        .to_string();
    for detail in ["page=2", "page_items=0", "collected=100", "total_count=101"] {
        assert!(error.contains(detail), "missing {detail}: {error}");
    }
    assert_eq!(server.count(), 3);
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

// A script makes unexpected extra requests (including page-local retries) fail.
async fn scripted_active_listing(
    responses: Vec<(u64, StatusCode, Value, Option<u64>)>,
) -> Result<MockGitHub> {
    let responses = Arc::new(Mutex::new(std::collections::VecDeque::from(responses)));
    MockGitHub::start(
        Router::new().route(
            "/repos/example/repo/actions/runs",
            get(move |Query(q): Query<HashMap<String, String>>| {
                let responses = Arc::clone(&responses);
                async move {
                    assert_eq!(q["status"], "in_progress");
                    assert_eq!(q["per_page"], "100");
                    let (page, status, body, remaining) = responses
                        .lock()
                        .unwrap()
                        .pop_front()
                        .expect("unexpected listing request");
                    assert_eq!(q["page"], page.to_string());
                    let mut headers = HeaderMap::new();
                    if let Some(remaining) = remaining {
                        headers.insert(
                            "x-ratelimit-remaining",
                            remaining.to_string().parse().unwrap(),
                        );
                    }
                    (status, headers, Json(body))
                }
            }),
        ),
        5000,
    )
    .await
}

fn listing(total: u64, ids: impl IntoIterator<Item = i64>) -> Value {
    json!({"total_count":total,"workflow_runs":ids.into_iter().map(|id| json!({"id":id})).collect::<Vec<_>>()})
}

#[tokio::test]
async fn active_listing_restarts_from_page_one_and_discards_failed_attempt() -> Result<()> {
    for responses in [
        vec![
            (1, StatusCode::OK, listing(2, [99]), None),
            (1, StatusCode::OK, listing(2, [1, 2]), None),
        ],
        vec![
            (1, StatusCode::OK, listing(101, 1000..1100), None),
            (2, StatusCode::OK, listing(101, []), None),
            (1, StatusCode::OK, listing(101, 0..100), None),
            (2, StatusCode::OK, listing(101, [100]), None),
        ],
    ] {
        let expected_requests = responses.len() + 1;
        let expected_ids: Vec<i64> = if responses.len() == 2 {
            vec![1, 2]
        } else {
            (0..101).collect()
        };
        let server = scripted_active_listing(responses).await?;
        let mut client = server.client(None, expected_requests as u64, 100).await?;
        let Pages::Complete(runs) =
            crate::reconcile::active_runs(&mut client, "example/repo", "in_progress").await?
        else {
            bail!("unexpected cap")
        };
        assert_eq!(
            runs.iter()
                .map(|r| r["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            expected_ids
        );
        assert_eq!(server.count(), expected_requests);
        assert_eq!(client.requests, expected_requests as u64);
    }
    Ok(())
}

#[tokio::test]
async fn active_listing_persistent_short_pages_stop_after_one_restart() -> Result<()> {
    let server = scripted_active_listing(vec![
        (1, StatusCode::OK, listing(2, [1]), None),
        (1, StatusCode::OK, listing(2, [2]), None),
    ])
    .await?;
    let mut client = server.client(None, 100, 100).await?;
    let error = crate::reconcile::active_runs(&mut client, "example/repo", "in_progress")
        .await
        .unwrap_err();
    assert!(error.is::<ShortPage>());
    let detail = format!("{error:#}");
    assert!(detail.contains("after one short-page restart"));
    assert!(detail.contains("collected=1, total_count=2"));
    assert_eq!(server.count(), 3);
    Ok(())
}

#[tokio::test]
async fn active_listing_restart_obeys_existing_budget_and_reserve() -> Result<()> {
    for (budget, remaining, message) in [
        (2, None, "request budget reached"),
        (20, Some(100), "primary rate-limit reserve reached"),
    ] {
        let server =
            scripted_active_listing(vec![(1, StatusCode::OK, listing(2, [1]), remaining)]).await?;
        let mut client = server.client(None, budget, 100).await?;
        let error = crate::reconcile::active_runs(&mut client, "example/repo", "in_progress")
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains(message));
        assert_eq!(server.count(), 2);
    }
    // The restart also stops mid-listing when it consumes the final budget slot.
    let server = scripted_active_listing(vec![
        (1, StatusCode::OK, listing(2, [1]), None),
        (1, StatusCode::OK, listing(101, 0..100), None),
    ])
    .await?;
    let mut client = server.client(None, 3, 100).await?;
    let error = crate::reconcile::active_runs(&mut client, "example/repo", "in_progress")
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("outbound_requests=3/3"));
    assert_eq!(server.count(), 3);
    Ok(())
}

#[tokio::test]
async fn active_listing_keeps_non_short_failures_and_search_cap_terminal() -> Result<()> {
    for prefix in [false, true] {
        for (failure, message) in [
            (
                vec![(1, StatusCode::OK, json!({"workflow_runs":[]}), None)],
                "missing total_count",
            ),
            (
                vec![(1, StatusCode::OK, listing(2, [1, 1]), None)],
                "duplicate item",
            ),
            (
                vec![(1, StatusCode::OK, listing(1, [1, 2]), None)],
                "more results",
            ),
            (
                vec![
                    (1, StatusCode::OK, listing(101, 0..100), None),
                    (2, StatusCode::OK, listing(102, [100, 101]), None),
                ],
                "result count changed",
            ),
            (
                vec![(1, StatusCode::TOO_MANY_REQUESTS, json!({}), None)],
                "429",
            ),
            (vec![(1, StatusCode::BAD_GATEWAY, json!({}), None)], "502"),
        ] {
            let mut responses = vec![];
            if prefix {
                responses.push((1, StatusCode::OK, listing(2, [1]), None));
            }
            responses.extend(failure);
            let count = responses.len() + 1;
            let server = scripted_active_listing(responses).await?;
            let mut client = server.client(None, 100, 100).await?;
            let error = crate::reconcile::active_runs(&mut client, "example/repo", "in_progress")
                .await
                .unwrap_err();
            assert!(format!("{error:#}").contains(message), "{error:#}");
            assert_eq!(server.count(), count);
        }
        let mut responses = vec![];
        if prefix {
            responses.push((1, StatusCode::OK, listing(2, [1]), None));
        }
        responses.push((1, StatusCode::OK, listing(1001, 0..100), None));
        let count = responses.len() + 1;
        let server = scripted_active_listing(responses).await?;
        let mut client = server.client(None, 100, 100).await?;
        assert!(matches!(
            crate::reconcile::active_runs(&mut client, "example/repo", "in_progress").await?,
            Pages::TooMany(1001)
        ));
        assert_eq!(server.count(), count);
    }
    Ok(())
}
