use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use github_actions_observer::{
    backfill::{BackfillOptions, backfill},
    config::Filters,
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
        #[arg(long, env = "INTERNAL_BIND", default_value = "0.0.0.0:9090")]
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
        .acquire_timeout(Duration::from_secs(2))
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
                },
            )
            .await?
        }
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
