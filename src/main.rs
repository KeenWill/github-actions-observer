use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use github_actions_observer::{
    backfill::{BackfillOptions, backfill},
    config::Filters,
    reconcile::{ReconcileOptions, reconcile},
    server::{AppState, serve},
    store::{self, Source},
};
use serde::Deserialize;
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Parser)]
#[command(version, about)]
struct Arguments {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,
    #[arg(long, env = "REPOSITORIES_INCLUDE", default_value = "")]
    repositories_include: String,
    #[arg(long, env = "REPOSITORIES_EXCLUDE", default_value = "")]
    repositories_exclude: String,
    #[arg(long, env = "EVENTS_INCLUDE", default_value = "")]
    events_include: String,
    #[arg(long, env = "EVENTS_EXCLUDE", default_value = "")]
    events_exclude: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply embedded database migrations using a schema-owning database role.
    Migrate,
    /// Listen for authenticated webhooks and expose internal metrics separately.
    Serve {
        #[arg(long, env = "GITHUB_WEBHOOK_SECRET", hide_env_values = true)]
        webhook_secret: String,
        #[arg(long, env = "WEBHOOK_BIND", default_value = "0.0.0.0:8080")]
        webhook_bind: SocketAddr,
        #[arg(long, env = "INTERNAL_BIND", default_value = "127.0.0.1:9090")]
        internal_bind: SocketAddr,
    },
    /// Import all attempts/jobs of runs CREATED in an inclusive UTC interval.
    Backfill {
        #[arg(long = "repository", required = true)]
        repositories: Vec<String>,
        #[arg(long)]
        created_since: DateTime<Utc>,
        #[arg(long)]
        created_until: DateTime<Utc>,
        #[arg(long, env = "GITHUB_TOKEN", hide_env_values = true)]
        token: String,
        #[arg(long, default_value = "https://api.github.com/")]
        api_base: reqwest::Url,
        /// Private immutable response snapshots; reuse the same directory to resume.
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        /// Maximum outbound GETs per invocation, including quota preflight; cache hits are free.
        #[arg(long, default_value_t = 1000)]
        max_requests: u64,
        /// Stop before consuming this many remaining primary API requests.
        #[arg(long, default_value_t = 100)]
        rate_limit_reserve: u64,
    },
    /// Refresh active workflow/job records and store verified queue snapshots.
    Reconcile {
        #[arg(long = "repository")]
        repositories: Vec<String>,
        #[arg(long, env = "GITHUB_TOKEN", hide_env_values = true, required_unless_present = "app_id", conflicts_with_all = ["app_id", "installation_id", "private_key_file"])]
        token: Option<String>,
        /// GitHub App client ID or application ID used as the JWT issuer.
        #[arg(long, env = "GITHUB_APP_ID", requires_all = ["installation_id", "private_key_file"])]
        app_id: Option<String>,
        #[arg(long, env = "GITHUB_APP_INSTALLATION_ID", requires = "app_id", value_parser=clap::value_parser!(u64).range(1..))]
        installation_id: Option<u64>,
        #[arg(long, env = "GITHUB_APP_PRIVATE_KEY_FILE", requires = "app_id")]
        private_key_file: Option<PathBuf>,
        #[arg(long, default_value = "https://api.github.com/")]
        api_base: reqwest::Url,
        #[arg(long, default_value_t = 1000)]
        max_requests: u64,
        #[arg(long, default_value_t = 500)]
        rate_limit_reserve: u64,
    },
    /// Record ARC pod-to-worker mappings using an in-cluster pod-reader service account.
    RunnerHosts {
        /// Repeat collection at this interval; omit for one collection.
        #[arg(long, value_parser=clap::value_parser!(u64).range(5..))]
        interval_seconds: Option<u64>,
    },
    /// Import local JSONL envelopes; stable delivery IDs make reruns idempotent.
    Replay {
        #[arg(long)]
        input: PathBuf,
    },
}

#[derive(Deserialize)]
struct ReplayEnvelope {
    delivery_id: String,
    event: String,
    payload: Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let arguments = Arguments::parse();
    let filters = Filters::new(
        &arguments.repositories_include,
        &arguments.repositories_exclude,
        &arguments.events_include,
        &arguments.events_exclude,
    )?;
    let mut database_options = PgConnectOptions::from_str(&arguments.database_url)?;
    if !matches!(&arguments.command, Command::Migrate) {
        database_options =
            database_options.options([("statement_timeout", "5000"), ("lock_timeout", "3000")]);
    }
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(
            if matches!(&arguments.command, Command::Serve { .. }) {
                2
            } else {
                30
            },
        ))
        .connect_with(database_options)
        .await
        .context("could not connect to PostgreSQL")?;
    match arguments.command {
        Command::Migrate => store::MIGRATOR.run(&pool).await?,
        Command::Serve {
            webhook_secret,
            webhook_bind,
            internal_bind,
        } => {
            serve(
                AppState::new(pool.clone(), webhook_secret, filters)?,
                webhook_bind,
                internal_bind,
            )
            .await?
        }
        Command::Backfill {
            repositories,
            created_since,
            created_until,
            token,
            api_base,
            cache_dir,
            max_requests,
            rate_limit_reserve,
        } => {
            backfill(
                &pool,
                &filters,
                BackfillOptions {
                    repositories,
                    created_since,
                    created_until,
                    token,
                    api_base,
                    cache_dir,
                    max_requests,
                    rate_limit_reserve,
                },
            )
            .await?
        }
        Command::Reconcile {
            repositories,
            token,
            app_id,
            installation_id,
            private_key_file,
            api_base,
            max_requests,
            rate_limit_reserve,
        } => {
            github_actions_observer::github_app::validate_api_base(&api_base)?;
            let token = match (token, app_id, installation_id, private_key_file) {
                (Some(token), None, None, None) => token,
                (None, Some(issuer), Some(installation), Some(key_file)) => {
                    github_actions_observer::github_app::installation_token(
                        &api_base,
                        &issuer,
                        installation,
                        &key_file,
                    )
                    .await?
                }
                _ => anyhow::bail!("supply either GITHUB_TOKEN or complete GitHub App credentials"),
            };
            reconcile(
                &pool,
                &filters,
                ReconcileOptions {
                    repositories,
                    token,
                    api_base,
                    max_requests,
                    rate_limit_reserve,
                },
            )
            .await?;
        }
        Command::RunnerHosts { interval_seconds } => loop {
            let result = github_actions_observer::runner_hosts::collect(&pool).await;
            if let Some(seconds) = interval_seconds {
                if result.is_err() {
                    tracing::error!(
                        "runner host collection failed; retrying at configured interval"
                    );
                }
                tokio::time::sleep(Duration::from_secs(seconds)).await;
            } else {
                result?;
                break;
            }
        },
        Command::Replay { input } => {
            let file = tokio::fs::File::open(input).await?;
            let mut lines = BufReader::new(file).lines();
            let mut count = 0_u64;
            while let Some(line) = lines.next_line().await? {
                if line.trim().is_empty() {
                    continue;
                }
                let envelope: ReplayEnvelope =
                    serde_json::from_str(&line).context("invalid replay envelope")?;
                if filters.accepts(&envelope.event, &envelope.payload) {
                    store::ingest(
                        &pool,
                        Source::Replay,
                        &envelope.delivery_id,
                        &envelope.event,
                        &envelope.payload,
                    )
                    .await?;
                    count += 1;
                }
            }
            tracing::info!(count, "replay complete");
        }
    }
    pool.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn parses(args: Vec<&str>) -> bool {
        let command = Arguments::command()
            .mut_arg("database_url", |arg| arg.env(None::<&str>))
            .mut_subcommand("reconcile", |mut command| {
                for name in ["token", "app_id", "installation_id", "private_key_file"] {
                    command = command.mut_arg(name, |arg| arg.env(None::<&str>));
                }
                command
            });
        command
            .try_get_matches_from(
                ["observer", "--database-url", "postgres://test", "reconcile"]
                    .into_iter()
                    .chain(args),
            )
            .is_ok()
    }

    #[test]
    fn reconcile_requires_exactly_one_complete_authentication_method() {
        for args in [
            vec!["--token", "test"],
            vec![
                "--app-id",
                "app",
                "--installation-id",
                "42",
                "--private-key-file",
                "/key.pem",
            ],
        ] {
            assert!(parses(args));
        }
        for args in [
            vec![],
            vec!["--app-id", "app"],
            vec!["--installation-id", "42"],
            vec!["--private-key-file", "/key.pem"],
            vec!["--token", "test", "--app-id", "app"],
            vec![
                "--app-id",
                "app",
                "--installation-id",
                "0",
                "--private-key-file",
                "/key.pem",
            ],
        ] {
            assert!(!parses(args));
        }
    }
}
