CREATE TABLE gha_deliveries (
    source text NOT NULL CHECK (source IN ('webhook', 'backfill', 'replay')),
    delivery_id text NOT NULL,
    event text NOT NULL,
    action text,
    repository text,
    payload jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    projection_status text NOT NULL CHECK (projection_status IN ('projected', 'unhandled', 'invalid')),
    PRIMARY KEY (source, delivery_id)
);
CREATE INDEX gha_deliveries_received_at ON gha_deliveries (received_at);

CREATE TABLE gha_workflow_runs (
    repository_id bigint NOT NULL,
    repository text NOT NULL,
    run_id bigint NOT NULL,
    run_attempt integer NOT NULL CHECK (run_attempt > 0),
    workflow_name text,
    branch text,
    event text NOT NULL,
    status text NOT NULL,
    status_rank smallint NOT NULL,
    conclusion text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    started_at timestamptz,
    html_url text NOT NULL,
    PRIMARY KEY (repository_id, run_id, run_attempt)
);
CREATE INDEX gha_workflow_runs_status ON gha_workflow_runs (status, repository);

CREATE TABLE gha_jobs (
    repository_id bigint NOT NULL,
    repository text NOT NULL,
    job_id bigint NOT NULL,
    run_id bigint NOT NULL,
    run_attempt integer NOT NULL CHECK (run_attempt > 0),
    job_name text NOT NULL,
    workflow_name text,
    branch text,
    status text NOT NULL,
    status_rank smallint NOT NULL,
    conclusion text,
    created_at timestamptz,
    started_at timestamptz,
    completed_at timestamptz,
    html_url text NOT NULL,
    runner_name text,
    runner_group_name text,
    runner_labels text[] NOT NULL,
    PRIMARY KEY (repository_id, job_id)
);
CREATE INDEX gha_jobs_completed_at ON gha_jobs (completed_at, repository);
CREATE INDEX gha_jobs_run ON gha_jobs (repository_id, run_id, run_attempt);
CREATE INDEX gha_jobs_active ON gha_jobs (status) WHERE status <> 'completed';

CREATE TABLE gha_steps (
    repository_id bigint NOT NULL,
    job_id bigint NOT NULL,
    step_number integer NOT NULL,
    step_name text NOT NULL,
    status text NOT NULL,
    conclusion text,
    started_at timestamptz,
    completed_at timestamptz,
    PRIMARY KEY (repository_id, job_id, step_number),
    FOREIGN KEY (repository_id, job_id) REFERENCES gha_jobs(repository_id, job_id) ON DELETE CASCADE
);

CREATE VIEW gha_job_history AS
SELECT j.*, r.event, COALESCE(j.workflow_name, r.workflow_name) AS workflow,
       EXTRACT(epoch FROM j.completed_at - j.started_at) AS duration_seconds,
       EXTRACT(epoch FROM j.started_at - j.created_at) AS queue_seconds
FROM gha_jobs j LEFT JOIN gha_workflow_runs r USING (repository_id, run_id, run_attempt);
