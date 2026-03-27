"""Mozart OpenSearch client for fetching jobs in an attribution window."""

import logging
from typing import Any, Iterator

from opensearchpy import OpenSearch

from .config import AttributionConfig, TERMINAL_STATUSES

logger = logging.getLogger(__name__)


def create_mozart_client(config: AttributionConfig) -> OpenSearch:
    """Create an OpenSearch client for Mozart."""
    return OpenSearch(
        hosts=[{"host": config.mozart_host, "port": config.mozart_port}],
        use_ssl=False,
        verify_certs=False,
        timeout=60,
    )


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
        "_source": [
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
            "resource_usage.cpu_total_ns",
            "resource_usage.memory_max_bytes",
        ],
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


def fetch_metrics_extra_attempts(
    config: AttributionConfig,
    mozart_jobs: list[dict],
) -> list[dict]:
    """Find execution attempts in metrics that Mozart overwrote.

    When a HySDS job fails and is retried, Mozart overwrites the payload_id
    document — the failed attempt vanishes. Metrics (logstash-* at port 9400,
    type=job_info) is append-only and preserves every execution.

    This function queries metrics for job_info docs matching the given job IDs,
    and returns only the "extra" attempts (the ones Mozart no longer has).

    Args:
        config: Attribution config with time window and cluster settings.
        mozart_jobs: Jobs already fetched from Mozart (used for ID matching
            and metadata inheritance).

    Returns:
        List of flattened job dicts for the extra attempts, enriched with
        metadata inherited from the corresponding Mozart record.
    """
    # Build lookup from job name to Mozart record
    mozart_by_name: dict[str, dict] = {}
    for job in mozart_jobs:
        payload_id = job.get("payload_id", "")
        if payload_id:
            # job name in metrics is the payload_id minus the timestamp suffix
            # but actually job.job_info.id in metrics == payload_id in Mozart
            mozart_by_name[payload_id] = job

    if not mozart_by_name:
        return []

    client = create_metrics_client(config)

    # Query logstash-* for job_info docs in the attribution window
    # We search by time range, then filter to known job IDs
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": "job_info"}},
                    {
                        "range": {
                            "@timestamp": {
                                "gte": config.window_start,
                                "lte": config.window_end,
                            }
                        }
                    },
                    {
                        "terms": {
                            "job.job_info.id": list(mozart_by_name.keys())
                        }
                    },
                ],
            }
        },
        "_source": [
            "job.job_info.id",
            "job.job_info.status",
            "job.job_info.time_queued",
            "job.job_info.time_start",
            "job.job_info.time_end",
            "job.job_info.duration",
            "job.job_info.job_queue",
            "job.job_info.facts.ec2_instance_id",
            "job.job_info.facts.ec2_instance_type",
            "job.job_info.facts.ec2_placement_availability_zone",
            "@timestamp",
        ],
        "size": 1000,
        "sort": [{"@timestamp": "asc"}],
    }

    # Scroll through all matching metrics docs
    metrics_docs: dict[str, list[dict]] = {}  # job_id -> list of metrics docs
    scroll_timeout = "5m"

    try:
        response = client.search(
            index="logstash-*",
            body=query,
            scroll=scroll_timeout,
        )
    except Exception as e:
        logger.warning("Metrics query failed: %s", e)
        return []

    scroll_id = response.get("_scroll_id")
    hits = response["hits"]["hits"]
    total = response["hits"]["total"]["value"]
    logger.info("Metrics query found %d job_info docs for %d job IDs", total, len(mozart_by_name))

    def _collect_hits(hits):
        for hit in hits:
            source = hit["_source"]
            job_id = source.get("job", {}).get("job_info", {}).get("id", "")
            if job_id:
                metrics_docs.setdefault(job_id, []).append(source)

    _collect_hits(hits)

    while hits:
        response = client.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
        scroll_id = response.get("_scroll_id")
        hits = response["hits"]["hits"]
        _collect_hits(hits)

    if scroll_id:
        try:
            client.clear_scroll(scroll_id=scroll_id)
        except Exception:
            pass

    # Identify extra attempts: for each job with N metrics docs, the last one
    # is the attempt Mozart kept; earlier ones are overwritten failed attempts.
    extra_attempts = []
    jobs_with_retries = 0

    for job_id, docs in metrics_docs.items():
        if len(docs) <= 1:
            continue  # Only one attempt — no extras

        jobs_with_retries += 1
        mozart_job = mozart_by_name.get(job_id)
        if not mozart_job:
            continue

        # Sort by @timestamp ascending — last is the final attempt
        docs.sort(key=lambda d: d.get("@timestamp", ""))

        # All except the last are overwritten attempts
        for attempt_idx, doc in enumerate(docs[:-1]):
            info = doc.get("job", {}).get("job_info", {})
            facts = info.get("facts", {})

            # Map metrics status code to HySDS status string
            status_code = info.get("status", 0)
            if status_code == 0:
                status = "job-completed"
            elif status_code == 1:
                status = "job-failed"
            elif status_code == 143:
                status = "job-failed"  # killed
            else:
                status = "job-failed"

            # Build a flattened job dict matching _flatten_facts() format
            synthetic_payload = f"{job_id}-attempt-{attempt_idx + 1}"
            attempt = {
                "payload_id": synthetic_payload,
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
                        "metrics": {},
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

    logger.info(
        "Found %d extra execution attempts from metrics (%d jobs had retries)",
        len(extra_attempts), jobs_with_retries,
    )

    return extra_attempts
