# Jobs Orchestration for Pipelines

> **Purpose**: Schedule and orchestrate declarative pipelines together with notebooks, SQL, and alerts using Lakeflow Jobs
> **MCP Validated**: 2026-06-10

## When to Use

- A pipeline must run on a schedule, on file arrival, or after upstream tasks
- The workflow mixes a pipeline with other steps (pre-checks, notebooks, SQL, dbt, alerts)
- You need retries, timeouts, notifications, and run history in one place

> Lakeflow Jobs is the current name of Databricks Workflows/Jobs. A pipeline itself orchestrates its internal dataset DAG; Jobs orchestrates **across** pipelines and other task types.

## Implementation

Pipeline execution modes (set on the pipeline, not the job):

| Mode | Behavior | Cost profile |
|------|----------|--------------|
| Triggered | Runs an update, processes available data, stops | Pay per update — default choice |
| Continuous | Stays running, ingests as data arrives | Always-on compute — low latency, higher cost |

Job definition (Databricks Asset Bundles YAML — same model as the UI):

```yaml
# resources/orders_job.yml
resources:
  jobs:
    orders_daily:
      name: orders-daily
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"   # 06:00 daily
        timezone_id: America/Sao_Paulo
        pause_status: UNPAUSED
      email_notifications:
        on_failure: [team@example.com]
      tasks:
        - task_key: preflight_check
          notebook_task:
            notebook_path: ../src/preflight.ipynb

        - task_key: run_pipeline
          depends_on: [{ task_key: preflight_check }]
          pipeline_task:
            pipeline_id: ${resources.pipelines.orders_pipeline.id}
            full_refresh: false
          max_retries: 2
          min_retry_interval_millis: 60000

        - task_key: publish_report
          depends_on: [{ task_key: run_pipeline }]
          sql_task:
            warehouse_id: ${var.warehouse_id}
            query: { query_id: ${var.report_query_id} }
```

Other useful trigger types (instead of `schedule`):

```yaml
      trigger:
        file_arrival:                  # run when new files land in a UC location
          url: /Volumes/main/landing/raw_files/orders/
        # or
        # periodic: { interval: 1, unit: HOURS }
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `pipeline_task.full_refresh` | `false` | `true` reprocesses all data (careful with non-replayable sources) |
| `max_retries` | 0 | Automatic retries per task on failure |
| `timeout_seconds` | none | Kill long-running task runs |
| `max_concurrent_runs` (job) | 1 | Allow/skip overlapping runs |
| `depends_on` | none | Task DAG edges; supports `run_if` conditions (e.g., `ALL_DONE`) |
| Trigger types | manual | `schedule` (cron), `file_arrival`, `periodic`, `continuous` |
| Compute | serverless | Serverless jobs compute by default; classic clusters on paid tiers |

Pipeline dev vs prod behavior:

- **Development mode**: reuses compute across runs for fast iteration, no automatic retries.
- **Production mode**: fresh compute per update, automatic retry on recoverable failures.

## Example Usage

Free Edition shape (limits: 1 active pipeline per type, 5 concurrent tasks, serverless only):

```text
job: nightly-etl (scheduled 02:00, max_concurrent_runs: 1)
└── task 1: pipeline_task → single pipeline containing the whole
            bronze → silver → gold DAG (triggered mode)
└── task 2: sql_task → refresh dashboard query (depends_on task 1)
```

Keep orchestration **flat and sequential** on Free Edition; split into multiple pipelines/jobs only on paid workspaces.

## See Also

- [free-edition-constraints](../concepts/free-edition-constraints.md)
- [autoloader-ingestion](../patterns/autoloader-ingestion.md)
- [auto-cdc-scd](../patterns/auto-cdc-scd.md)
