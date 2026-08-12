# HySDS Job Cost Attribution

Attributes AWS compute costs to individual HySDS jobs by correlating AWS Cost Explorer data with job records in Mozart (OpenSearch).

## Prerequisites

- Python 3.10+
- AWS credentials configured (via environment variables, `~/.aws/credentials`, or IAM role)
- Read access to Mozart OpenSearch (job index at `job_status-*`)
- Access to Redis (default publish path) **or** metrics OpenSearch (direct publish)

## Installation

```bash
git clone <repo-url>
cd hysds-job-cost-attribution
python -m venv .venv
source .venv/bin/activate
pip install -r cost_attribution/requirements.txt
```

## Configuration

Connection defaults (override via environment variables — useful when running against a remote cluster, e.g. in a CI/cron job that isn't co-located with Mozart/Metrics/Redis):

| Service | Default | Env var(s) |
|---|---|---|
| Mozart OpenSearch | `localhost:9300` | `MOZART_HOST`, `MOZART_PORT` |
| Metrics OpenSearch | `localhost:9400` | `METRICS_HOST`, `METRICS_PORT` |
| Redis host | `127.0.0.1:6379` | `REDIS_HOST`, `REDIS_PORT` |
| Redis password | none | `REDIS_PASSWORD` (or `--redis-password`) |
| AWS region | `us-west-2` | `--region` flag |

AWS credentials are resolved via the standard boto3 chain (env vars → `~/.aws/credentials` → IAM role). The IAM role or user needs `ce:GetCostAndUsage` and EC2/Pricing read permissions.

## Usage

### Single day (defaults to 3 days ago)

```bash
python -m cost_attribution
```

### Specific date

```bash
python -m cost_attribution --date 2026-02-15
```

### Date range backfill

Makes a single Cost Explorer API call for the full range, then attributes costs per-day:

```bash
python -m cost_attribution --date-range 2026-02-01 2026-02-28
```

### Hourly estimator

Scans the last 48 hours for terminal jobs without a cost document yet, and estimates costs using spot/on-demand pricing (no Cost Explorer call):

```bash
python -m cost_attribution --estimate-recent
```

---

## Options Reference

### `--date YYYY-MM-DD`

**Default:** none — falls back to 3 days ago (T-3).

The date to attribute costs for. The attribution window covers `00:00:00Z` to `23:59:59Z` on that day. Mutually exclusive with `--date-range`.

Use this for daily scheduled runs or to reprocess a single specific day.

---

### `--date-range START END`

**Default:** none.

Backfill a span of days (both dates inclusive). The tool fetches a single Cost Explorer call for the entire range and then iterates day by day. Mutually exclusive with `--date`.

```bash
python -m cost_attribution --date-range 2026-01-01 2026-01-31
```

Use this instead of running `--date` in a loop — it's far more efficient against the CE API.

---

### `--estimate-recent`

**Default:** off.

Hourly estimator mode. Scans jobs that ended in the last 48 hours, skips any that already have a cost document, and estimates costs using live spot/on-demand pricing rates instead of Cost Explorer. Designed to run frequently (e.g., every hour via cron) to give near-real-time cost visibility before the daily CE data is available.

---

### `--asg-prefix PREFIX`

**Default:** `""` (empty — no prefix stripped).

The prefix to strip from AWS Auto Scaling Group names before matching them to HySDS queue names. Cost Explorer returns costs tagged by ASG name; this prefix is removed to recover the bare queue name.

Example: with `--asg-prefix smap-fwd-v1-`, an ASG named `smap-fwd-v1-radar-workflow` becomes `radar-workflow` for matching.

---

### `--queue-map-file FILE`

**Default:** none — ASG names are matched to job queues directly after prefix stripping.

Path to a JSON file that maps Cost Explorer/ASG tag names (after prefix stripping) to HySDS queue names. Use this when the CE tag names and the queue names in Mozart don't match exactly.

See [Queue Map File](#queue-map-file) for format details.

---

### `--region REGION`

**Default:** `us-west-2`.

AWS region used for Cost Explorer and EC2 pricing API calls. Must match the region where your HySDS cluster runs.

---

### `--cost-metric METRIC`

**Default:** `AmortizedCost`.

The AWS Cost Explorer cost metric to use. Options:

| Metric | When to use |
|---|---|
| `AmortizedCost` | **Recommended.** Spreads reserved instance upfront fees across the reservation period. Best for showing the true cost per day. |
| `UnblendedCost` | Shows the actual charge on your bill for that day, including any RI upfront fees when they occur. |
| `BlendedCost` | Averages costs across the consolidated billing family. Useful if your account is part of an AWS Organization. |

---

### `--use-fallback`

**Default:** off.

Skip Cost Explorer entirely and estimate job costs using EC2 spot/on-demand pricing rates instead. Useful when:
- CE data isn't available yet for the target date
- You lack `ce:GetCostAndUsage` IAM permission
- You want a rough estimate without waiting for CE finalization

When fallback is active, costs are estimated from the job's recorded instance type and duration. Queue overhead documents are not produced.

---

### `--include-retries`

**Default:** off.

Query the metrics OpenSearch index for retry attempts that were overwritten in Mozart (i.e., the job was retried and the original attempt record no longer appears in `job_status-*`). These extra attempts are included in cost attribution so that retry compute is not silently ignored.

---

### `--skip-job-types PREFIX`

**Default:** none (all job types are attributed).

Exclude jobs whose type starts with the given prefix. Repeatable — pass it multiple times to skip several prefixes.

```bash
--skip-job-types job-ANTAZ_PP --skip-job-types job-PRECIP_PP
```

Useful when certain job types (e.g., lightweight pre-processing workflows) should not pollute the cost report, or when a job type is known to have bad cost data.

---

### `--direct`

**Default:** off — publishes via Redis.

Bulk-index cost documents directly to the metrics OpenSearch cluster, bypassing Redis and Logstash. Use this when you don't have a Logstash pipeline or want to write documents immediately without queuing.

See [Publish Modes](#publish-modes) for more detail.

---

### `--redis`

**Default:** on (this is the default publish mode).

Publish documents to Redis under the key `cost-attribution`, where a Logstash pipeline picks them up and writes them to OpenSearch. Explicit flag is only needed to override a previous `--direct` setting.

---

### `--redis-password PASSWORD`

**Default:** none.

Password for Redis authentication. Can also be set via the `REDIS_PASSWORD` environment variable (the env var takes the same effect as passing the flag).

```bash
export REDIS_PASSWORD=mysecret
python -m cost_attribution
```

---

### `--dry-run`

**Default:** off.

Calculate costs and build all documents, but skip the publish step. Nothing is written to Redis or OpenSearch. The summary is still printed to stdout. Use this to verify configuration and cost calculations before committing to a live run.

---

### `-v` / `--verbose`

**Default:** off.

Enable `DEBUG`-level logging for `cost_attribution` modules. Shows per-queue CE cost matching, per-job cost breakdown, and unmatched ASG names. Third-party libraries (boto3, opensearch-py, urllib3) remain at INFO to avoid noise.

---

## Walkthrough: First Run

This section walks through a safe first run on a single day.

### Step 1 — Verify AWS credentials

```bash
aws sts get-caller-identity
```

You should see your account ID and role/user ARN. If this fails, configure credentials before proceeding.

### Step 2 — Dry run for yesterday's T-3 date

Run without publishing to confirm connectivity and see what the output looks like:

```bash
python -m cost_attribution --dry-run --verbose
```

Expected output (truncated):

```
2026-05-13T10:00:01 [INFO] cost_attribution.main: Starting cost attribution for 2026-05-13
2026-05-13T10:00:01 [INFO] cost_attribution.main: Run ID: 2026-05-13-a1b2c3d4
2026-05-13T10:00:01 [INFO] cost_attribution.main: Window: 2026-05-13T00:00:00Z → 2026-05-13T23:59:59Z
...

Cost Attribution Summary for 2026-05-13
=============================================
Cost metric:       AmortizedCost
Jobs processed:    142
Total job cost:    $18.4210
Total overhead:    $2.1340
CE total (tag):    $20.5550
Queues:            5
Documents:         147
Run ID:            2026-05-13-a1b2c3d4
Mode:              DRY RUN (not published)
```

**Understanding the summary:**

| Field | Meaning |
|---|---|
| `Jobs processed` | Terminal jobs (completed, failed, deduped, revoked) found in Mozart for the window |
| `Total job cost` | Sum of EC2 + EBS costs allocated to individual jobs |
| `Total overhead` | CE queue total minus attributed job costs (idle instance time, spin-up, etc.) |
| `CE total (tag)` | Total cost reported by Cost Explorer for matched worker queues |
| `Queues` | Number of distinct HySDS queues with jobs in this window |
| `Documents` | Job cost docs + queue overhead docs that would be published |
| `Run ID` | Unique identifier for this run; used for deduplication |

### Step 3 — Check ASG/queue matching (verbose)

If `Total job cost` is $0 or far below expected, the ASG names from CE likely aren't matching the queue names from Mozart. Run with `-v` and look for lines like:

```
CE queue names MATCHED (3):
  radar-workflow                                     $12.3400
CE queue names UNMATCHED (8, $45.2100 total):
  pcm-factotum                                       $20.1000
  ...
Job queues with NO CE match (2):
  hysds-job-worker-small  (3420.0 job-seconds)
```

If your ASG names have a prefix (e.g., `smap-fwd-v1-radar-workflow`), add `--asg-prefix smap-fwd-v1-`. If the stripped names still don't match queue names in Mozart, create a queue map file and pass it with `--queue-map-file`.

### Step 4 — Publish for real

Once the dry run looks correct, remove `--dry-run`:

```bash
python -m cost_attribution --verbose
```

Documents are pushed to Redis. Logstash will pick them up and write them to the `job-cost-attribution-YYYY.MM.DD` index in OpenSearch.

To publish directly to OpenSearch instead:

```bash
python -m cost_attribution --direct --verbose
```

---

## Queue Map File

The queue map JSON file maps Cost Explorer/ASG tag values (after prefix stripping) to HySDS queue names. Two maps are included:

- [cost_attribution/queue_map.json](cost_attribution/queue_map.json) — default MAAP DPS queues
- [cost_attribution/queue_map.fwd.json](cost_attribution/queue_map.fwd.json) — SMAP FWD deployment

Example structure:

```json
{
  "ce-tag-name-after-prefix-strip": "hysds-queue-name-in-mozart"
}
```

If `--queue-map-file` is not provided, the tool attempts to match CE ASG names to Mozart queue names directly (after prefix stripping).

---

## Publish Modes

**Redis (default):** Documents are msgpack-encoded and pushed to a Redis list key (`cost-attribution`). A Logstash pipeline consumes the queue and writes to the metrics OpenSearch index (`job-cost-attribution-YYYY.MM.DD`).

**Direct (`--direct`):** Documents are bulk-indexed straight to the metrics OpenSearch cluster (port `9400` by default), bypassing Redis and Logstash. Useful for environments without a Logstash pipeline or when you need immediate writes.

---

## Logstash Setup (Redis publish mode)

The Redis publish mode requires two changes to the existing `metrics/etc/indexer.conf`.

### 1 — Add a third Redis input

In the `input { }` block, alongside the existing `logstash` and `sdswatch` inputs, add:

```
redis {
  host      => "127.0.0.1"
  password  => "xxx"
  data_type => "list"
  key       => "cost-attribution"
  codec     => msgpack
  # No add_field — @index is already set in the published document
}
```

### 2 — Update the output block

Replace the existing `output { }` block with one that routes `job-cost-attribution-*` documents using `document_id` (required for idempotent writes / deduplication):

```
output {
  if [@index] =~ /^job-cost-attribution/ {
    opensearch {
      hosts       => ["http://127.0.0.1:9200"]
      index       => "%{[@index]}"
      document_id => "%{[doc_id]}"
      action      => "index"
    }
  } else {
    opensearch {
      hosts => ["http://127.0.0.1:9200"]
      index => "%{[@index]}"
    }
  }
}
```

### Full resulting conf

```
input {
  redis {
    host     => "127.0.0.1"
    password => "xxx"
    data_type => "list"
    key      => "logstash"
    codec    => msgpack
    add_field => {
      "@index" => "logstash-%{+yyyy.MM.dd}"
    }
  }

  redis {
    host      => "127.0.0.1"
    password  => "xxx"
    data_type => "list"
    key       => "sdswatch"
    add_field => {
      "@index" => "sdswatch-%{+yyyy.MM.dd}"
    }
  }

  redis {
    host      => "127.0.0.1"
    password  => "xxx"
    data_type => "list"
    key       => "cost-attribution"
    codec     => msgpack
    # No add_field — @index is already set in the document
  }
}

output {
  if [@index] =~ /^job-cost-attribution/ {
    opensearch {
      hosts       => ["http://127.0.0.1:9200"]
      index       => "%{[@index]}"
      document_id => "%{[doc_id]}"
      action      => "index"
    }
  } else {
    opensearch {
      hosts => ["http://127.0.0.1:9200"]
      index => "%{[@index]}"
    }
  }
}
```

---

## SMAP FWD Example

Backfill February 2026 costs for the SMAP FWD deployment, skipping lightweight pre-processing job types, with retry attempts included:

```bash
python -m cost_attribution.main \
  --date-range 2026-02-01 2026-02-28 \
  --asg-prefix smap-fwd-v1- \
  --queue-map-file cost_attribution/queue_map.fwd.json \
  --skip-job-types job-ANTAZ_PP \
  --skip-job-types job-PRECIP_PP \
  --skip-job-types job-SNOW_PP \
  --skip-job-types job-TSURF_PP \
  --skip-job-types job-Enhanced \
  --skip-job-types job-Sentinel_L2_S0_S1 \
  --skip-job-types job-Sentinel_L2_SM_SP \
  --skip-job-types job-Radiometer \
  --skip-job-types job-Radar \
  --include-retries \
  --redis-password <password> \
  --dry-run \
  -v
```

Remove `--dry-run` to publish documents once the output looks correct.
