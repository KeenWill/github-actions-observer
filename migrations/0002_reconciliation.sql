ALTER TABLE gha_deliveries DROP CONSTRAINT gha_deliveries_source_check;
ALTER TABLE gha_deliveries ADD CONSTRAINT gha_deliveries_source_check
    CHECK (source IN ('webhook', 'backfill', 'replay', 'reconcile'));

-- Only complete API queue listings produce a snapshot; absent snapshots are unknown.
CREATE TABLE gha_queue_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository text NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX gha_queue_snapshots_repository_time ON gha_queue_snapshots(repository, checked_at DESC);
CREATE TABLE gha_queue_snapshot_runs (
    snapshot_id bigint NOT NULL REFERENCES gha_queue_snapshots(id),
    run_id bigint NOT NULL,
    run_attempt integer NOT NULL,
    workflow_name text,
    branch text,
    status text NOT NULL,
    html_url text NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (snapshot_id, run_id)
);
CREATE TABLE gha_reconciled_runs (
    repository_id bigint NOT NULL,
    run_id bigint NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_id, run_id)
);
CREATE TABLE gha_runner_hosts (
    runner_name text NOT NULL,
    pod_uid text NOT NULL,
    namespace text NOT NULL,
    worker_node text NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (runner_name, pod_uid)
);
CREATE INDEX gha_runner_hosts_runner ON gha_runner_hosts(runner_name);
ALTER TABLE gha_runner_hosts ADD COLUMN pod_created_at timestamptz NOT NULL;
CREATE VIEW gha_job_execution AS
SELECT j.*, h.namespace AS runner_namespace, h.worker_node, h.pod_uid,
       h.first_seen_at AS host_first_seen_at, h.last_seen_at AS host_last_seen_at
FROM gha_job_history j LEFT JOIN LATERAL (
    SELECT h.* FROM gha_runner_hosts h
    WHERE h.runner_name = j.runner_name
      AND j.started_at >= h.pod_created_at
      AND j.started_at <= h.last_seen_at
    ORDER BY h.pod_created_at DESC LIMIT 1
) h ON true;
