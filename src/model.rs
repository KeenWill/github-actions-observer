use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(transparent)]
pub struct GitHubId(pub i64);

#[derive(Debug, Deserialize)]
pub struct Repository {
    pub id: GitHubId,
    pub full_name: String,
}

#[derive(Debug, Deserialize)]
pub struct WorkflowRun {
    pub id: GitHubId,
    pub run_attempt: i32,
    pub name: Option<String>,
    pub head_branch: Option<String>,
    pub event: String,
    pub status: String,
    pub conclusion: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub run_started_at: Option<DateTime<Utc>>,
    pub html_url: String,
}

#[derive(Debug, Deserialize)]
pub struct Job {
    pub id: GitHubId,
    pub run_id: GitHubId,
    pub run_attempt: i32,
    pub name: String,
    pub workflow_name: Option<String>,
    pub head_branch: Option<String>,
    pub status: String,
    pub conclusion: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub html_url: String,
    pub runner_name: Option<String>,
    pub runner_group_name: Option<String>,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub steps: Vec<Step>,
}

#[derive(Debug, Deserialize)]
pub struct Step {
    pub number: i32,
    pub name: String,
    pub status: String,
    pub conclusion: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Unknown states are preserved, but cannot regress known active or terminal states.
pub fn status_rank(status: &str) -> i16 {
    match status {
        "completed" => 3,
        "in_progress" => 2,
        "queued" | "waiting" | "pending" | "requested" => 1,
        _ => 0,
    }
}
