"""Mozart OpenSearch client for fetching jobs in an attribution window."""

import logging
import re
from datetime import timedelta
from typing import Any, Iterator

from opensearchpy import OpenSearch

from .config import (
    AttributionConfig,
    METRICS_RETRY_PAD_HOURS,
    MOZART_ID_BATCH_SIZE,
    TERMINAL_STATUSES,
)
from .utils import iso_to_datetime

logger = logging.getLogger(__name__)


def create_mozart_client(config: AttributionConfig) -> OpenSearch:
    """Create an OpenSearch client for Mozart."""
    return OpenSearch(
        hosts=[{"host": config.mozart_host, "port": config.mozart_port}],
        use_ssl=False,
        verify_certs=False,
        timeout=60,
    )


# Fields fetched for every job record (window scan and by-ID lookup)
JOB_SOURCE_FIELDS = [
    "payload_id",
    "type",
    "status",
    "job.username",
    "job.tag",
    "job.job_info.job_queue",
    "job.job_info.time_queued",
    "job.job_info.time_start",
    "job.job_info.time_end",
    "job.job_info.duration",
    "job.job_info.facts.ec2_instance_id",
    "job.job_info.facts.ec2_instance_type",
    "job.job_info.facts.ec2_placement_availability_zone",
    "container_image_name",
    "job.job_info.metrics.products_staged.disk_usage",
    "job.job_info.metrics.products_staged.time_end",
    "resource_usage.cpu_total_ns",
    "resource_usage.memory_max_bytes",
]


def build_jobs_query(window_start: str, window_end: str) -> dict:
    """Build query for jobs that ended within the attribution window.

    Matches jobs with time_end in the window and terminal status.
    """
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "job.job_info.time_end": {
                                "gte": window_start,
                                "lte": window_end,
                            }
                        }
                    }
                ],
                "filter": [
                    {
                        "terms": {
                            "status": TERMINAL_STATUSES
                        }
                    }
                ],
            }
        },
        "_source": JOB_SOURCE_FIELDS,
    }


def _flatten_facts(source: dict) -> dict:
    """Promote job.job_info.facts EC2 fields to top level for downstream access."""
    facts = source.get("job", {}).get("job_info", {}).get("facts", {})
    for key in ("ec2_instance_id", "ec2_instance_type", "ec2_placement_availability_zone"):
        if key in facts:
            source[key] = facts[key]
    return source


def scroll_jobs(
    client: OpenSearch,
    config: AttributionConfig,
    scroll_size: int = 1000,
    scroll_timeout: str = "5m",
) -> Iterator[dict[str, Any]]:
    """Scroll through all jobs in the attribution window.

    Uses OpenSearch scroll API to handle large result sets.
    Yields raw _source documents.
    """
    query = build_jobs_query(config.window_start, config.window_end)
    query["size"] = scroll_size

    logger.info(
        "Querying Mozart for jobs ending between %s and %s",
        config.window_start,
        config.window_end,
    )

    response = client.search(
        index=config.job_index,
        body=query,
        scroll=scroll_timeout,
    )

    scroll_id = response.get("_scroll_id")
    hits = response["hits"]["hits"]
    total = response["hits"]["total"]["value"]
    logger.info("Found %d jobs in attribution window", total)

    for hit in hits:
        yield _flatten_facts(hit["_source"])

    while hits:
        response = client.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
        scroll_id = response.get("_scroll_id")
        hits = response["hits"]["hits"]
        for hit in hits:
            yield _flatten_facts(hit["_source"])

    # Clean up scroll context
    if scroll_id:
        try:
            client.clear_scroll(scroll_id=scroll_id)
        except Exception:
            pass


def fetch_all_jobs(config: AttributionConfig) -> list[dict[str, Any]]:
    """Fetch all jobs in the attribution window as a list."""
    client = create_mozart_client(config)
    jobs = list(scroll_jobs(client, config))
    logger.info("Fetched %d jobs total", len(jobs))
    return jobs


def create_metrics_client(config: AttributionConfig) -> OpenSearch:
    """Create an OpenSearch client for the Metrics cluster."""
    return OpenSearch(
        hosts=[{"host": config.metrics_host, "port": config.metrics_port}],
        use_ssl=False,
        verify_certs=False,
        timeout=60,
    )




def fetch_jobs_by_ids(
    config: AttributionConfig,
    payload_ids: list[str],
) -> dict[str, dict[str, Any]]:
    """Fetch Mozart job records by payload_id, regardless of status or end time.

    Queries in batches of MOZART_ID_BATCH_SIZE so large ID sets never exceed
    the OpenSearch terms-query limit.

    Returns:
        Dict mapping payload_id -> flattened job record.
    """
    if not payload_ids:
        return {}

    client = create_mozart_client(config)
    found: dict[str, dict[str, Any]] = {}

    for i in range(0, len(payload_ids), MOZART_ID_BATCH_SIZE):
        batch = payload_ids[i : i + MOZART_ID_BATCH_SIZE]
        response = client.search(
            index=config.job_index,
            body={
                "query": {"terms": {"payload_id": batch}},
                "_source": JOB_SOURCE_FIELDS,
                "size": len(batch),
            },
        )
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            payload_id = source.get("payload_id", "")
            if payload_id and payload_id not in found:
                found[payload_id] = _flatten_facts(source)

    return found


def _scroll_metrics_job_info(
    client: OpenSearch,
    timestamp_start: str,
    timestamp_end: str,
    scroll_timeout: str = "5m",
) -> Iterator[dict[str, Any]]:
    """Scroll all type=job_info docs in the metrics cluster within a @timestamp range."""
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": "job_info"}},
                    {
                        "range": {
                            "@timestamp": {
                                "gte": timestamp_start,
                                "lte": timestamp_end,
                            }
                        }
                    },
                ],
            }
        },
        "_source": [
            "job.job_info.job_payload.payload_task_id",
            "job.job_info.status",
            "job.job_info.time_queued",
            "job.job_info.time_start",
            "job.job_info.time_end",
            "job.job_info.duration",
            "job.job_info.job_queue",
            "job.job_info.facts.ec2_instance_id",
            "job.job_info.facts.ec2_instance_type",
            "job.job_info.facts.ec2_placement_availability_zone",
            "job.job_info.metrics.products_staged.time_end",
            "@timestamp",
        ],
        "size": 1000,
    }

    response = client.search(index="logstash-*", body=query, scroll=scroll_timeout)
    scroll_id = response.get("_scroll_id")
    hits = response["hits"]["hits"]
    logger.info(
        "Metrics query found %d job_info docs between %s and %s",
        response["hits"]["total"]["value"], timestamp_start, timestamp_end,
    )

    try:
        while hits:
            for hit in hits:
                yield hit["_source"]
            response = client.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
            scroll_id = response.get("_scroll_id")
            hits = response["hits"]["hits"]
    finally:
        if scroll_id:
            try:
                client.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass


def _attempt_payload_id(job_id: str, time_start: str | None) -> str:
    """Stable synthetic ID for an overwritten attempt, so reruns overwrite rather than duplicate."""
    start_digits = re.sub(r"\D", "", time_start or "") or "unknown"
    return f"{job_id}-attempt-{start_digits}"


def _same_instant(a: str, b: str) -> bool:
    """Compare two ISO timestamps as instants, tolerating format differences."""
    if a == b:
        return True
    if not a or not b:
        return False
    try:
        return iso_to_datetime(a) == iso_to_datetime(b)
    except (ValueError, TypeError):
        return False


def fetch_metrics_extra_attempts(
    config: AttributionConfig,
    mozart_jobs: list[dict],
) -> list[dict]:
    """Find execution attempts in metrics that Mozart overwrote.

    When a HySDS job fails and is retried, Mozart overwrites the payload_id
    document — the failed attempt vanishes. Metrics (logstash-* at port 9400,
    type=job_info) is append-only and preserves every execution.

    Every job_info doc whose attempt ended inside the attribution window is
    considered, not only those belonging to jobs whose final attempt also ended
    in the window. An attempt is "extra" when its time_start differs from the
    attempt Mozart currently holds for that job. Each extra attempt is costed
    against the day it ended, the same rule used for Mozart jobs.

    Args:
        config: Attribution config with time window and cluster settings.
        mozart_jobs: Jobs already fetched from Mozart for the window. Other
            jobs referenced by metrics are looked up by ID.

    Returns:
        List of flattened job dicts for the extra attempts, enriched with
        metadata inherited from the corresponding Mozart record.
    """
    window_start = iso_to_datetime(config.window_start)
    window_end = iso_to_datetime(config.window_end)
    pad = timedelta(hours=METRICS_RETRY_PAD_HOURS)
    timestamp_start = (window_start - pad).strftime("%Y-%m-%dT%H:%M:%SZ")
    timestamp_end = (window_end + pad).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Collect attempts that ended inside the window, deduped by (job, start time)
    attempts: dict[tuple[str, str], dict] = {}
    try:
        client = create_metrics_client(config)
        for source in _scroll_metrics_job_info(client, timestamp_start, timestamp_end):
            info = source.get("job", {}).get("job_info", {})
            job_id = info.get("job_payload", {}).get("payload_task_id", "")
            time_end = info.get("time_end")
            if not job_id or not time_end:
                continue
            try:
                ended = iso_to_datetime(time_end)
            except (ValueError, TypeError):
                continue
            if not (window_start <= ended <= window_end):
                continue
            attempts.setdefault((job_id, info.get("time_start") or ""), info)
    except Exception as e:
        logger.warning("Metrics query failed, retry attempts not included: %s", e)
        return []

    # Resolve the current Mozart record for every job referenced by an attempt
    mozart_by_id: dict[str, dict] = {
        job["payload_id"]: job for job in mozart_jobs if job.get("payload_id")
    }
    missing_ids = sorted({job_id for job_id, _ in attempts} - set(mozart_by_id))
    if missing_ids:
        try:
            mozart_by_id.update(fetch_jobs_by_ids(config, missing_ids))
        except Exception as e:
            logger.warning("Mozart lookup for %d job IDs failed: %s", len(missing_ids), e)

    extra_attempts = []
    jobs_with_retries: set[str] = set()
    skipped_no_mozart = 0

    for (job_id, time_start), info in attempts.items():
        mozart_job = mozart_by_id.get(job_id)
        if not mozart_job:
            skipped_no_mozart += 1
            continue

        # The attempt Mozart still holds is already costed from Mozart
        mozart_start = mozart_job.get("job", {}).get("job_info", {}).get("time_start") or ""
        if _same_instant(time_start, mozart_start):
            continue

        jobs_with_retries.add(job_id)
        facts = info.get("facts", {})

        # Map metrics status code to HySDS status string
        status = "job-completed" if info.get("status", 0) == 0 else "job-failed"

        # Build a flattened job dict matching _flatten_facts() format
        attempt = {
            "payload_id": _attempt_payload_id(job_id, time_start),
            "type": mozart_job.get("type", ""),
            "status": status,
            "job": {
                "username": mozart_job.get("job", {}).get("username", ""),
                "tag": mozart_job.get("job", {}).get("tag", ""),
                "job_info": {
                    "job_queue": info.get("job_queue", "")
                        or mozart_job.get("job", {}).get("job_info", {}).get("job_queue", ""),
                    "time_queued": info.get("time_queued"),
                    "time_start": info.get("time_start"),
                    "time_end": info.get("time_end"),
                    "duration": info.get("duration", 0) or 0,
                    "facts": facts,
                    # Only upload end times (for the run interval); no
                    # disk_usage, so failed attempts aren't charged S3 cost
                    "metrics": {"products_staged": [
                        {"time_end": p.get("time_end")}
                        for p in (info.get("metrics", {}) or {}).get("products_staged") or []
                        if isinstance(p, dict) and p.get("time_end")
                    ]},
                },
            },
            "container_image_name": mozart_job.get("container_image_name", ""),
            "resource_usage": {},
            "is_retry_attempt": True,
        }

        # Flatten EC2 facts to top level (same as _flatten_facts)
        for key in ("ec2_instance_id", "ec2_instance_type", "ec2_placement_availability_zone"):
            if key in facts:
                attempt[key] = facts[key]

        extra_attempts.append(attempt)

    if skipped_no_mozart:
        logger.warning(
            "Skipped %d metrics attempts with no Mozart record (no owner to attribute to)",
            skipped_no_mozart,
        )
    logger.info(
        "Found %d extra execution attempts from metrics (%d jobs had retries)",
        len(extra_attempts), len(jobs_with_retries),
    )

    return extra_attempts
