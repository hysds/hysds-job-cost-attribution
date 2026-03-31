"""Utility functions for cost attribution."""

import json
import logging
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def parse_job_type(job_type: str) -> tuple[str, str]:
    """Parse algorithm name and version from job type string.

    Examples:
        "job-algorithm:1.0.0" -> ("algorithm", "1.0.0")
        "job-my_algo:2.1" -> ("my_algo", "2.1")
        "job-algorithm" -> ("algorithm", "unknown")
    """
    # Strip "job-" prefix if present
    name = job_type
    if name.startswith("job-"):
        name = name[4:]

    # Split on last colon for version
    if ":" in name:
        parts = name.rsplit(":", 1)
        return parts[0], parts[1]
    return name, "unknown"


def asg_name_to_queue(asg_name: str, prefix: str = "") -> str:
    """Map ASG name to HySDS queue name by stripping the venue prefix.

    ASG naming: {venue-prefix}-{queue-name}
    Example: "maap-ops-v5-maap-dps-worker-32gb" with prefix "maap-ops-v5-"
             -> "maap-dps-worker-32gb"

    If no prefix given, return the full ASG name.
    """
    if prefix and asg_name.startswith(prefix):
        return asg_name[len(prefix):]
    return asg_name


def compute_s3_cost(output_bytes: int, num_products: int,
                    storage_rate_per_gb_month: float = 0.023,
                    put_rate_per_1000: float = 0.005,
                    days_per_month: int = 30) -> float:
    """Calculate S3 cost for job output products.

    Includes:
    - Storage cost: prorated daily from monthly GB rate
    - PUT request cost: per 1000 requests
    """
    output_gb = output_bytes / (1024 ** 3)
    storage_cost = output_gb * storage_rate_per_gb_month / days_per_month
    put_cost = num_products * put_rate_per_1000 / 1000
    return round(storage_cost + put_cost, 6)


def format_date_index(dt: datetime) -> str:
    """Format a datetime to OpenSearch monthly index suffix: YYYY.MM"""
    return dt.strftime("%Y.%m")


def iso_to_datetime(iso_str: str) -> datetime:
    """Parse ISO 8601 string to timezone-aware datetime."""
    # Handle both Z and +00:00 suffixes
    if iso_str.endswith("Z"):
        iso_str = iso_str[:-1] + "+00:00"
    return datetime.fromisoformat(iso_str)


def filter_skipped_job_types(
    jobs: list[dict[str, Any]],
    skip_job_types: tuple[str, ...] | list[str],
) -> list[dict[str, Any]]:
    """Filter out jobs whose type starts with any of the given prefixes.

    Args:
        jobs: List of job documents.
        skip_job_types: Job type prefixes to skip (e.g., "job-workflow-orchestrator").
            A job is skipped if its "type" field starts with any prefix.

    Returns:
        Filtered list of jobs (skipped jobs removed).
    """
    if not skip_job_types:
        return jobs

    kept = []
    skipped_counts: Counter[str] = Counter()
    for job in jobs:
        job_type = job.get("type", "")
        if any(job_type.startswith(prefix) for prefix in skip_job_types):
            skipped_counts[job_type] += 1
        else:
            kept.append(job)

    total_skipped = sum(skipped_counts.values())
    if total_skipped:
        logger.info(
            "Skipped %d jobs matching --skip-job-types: %s",
            total_skipped,
            ", ".join(f"{t} ({n})" for t, n in skipped_counts.most_common()),
        )

    return kept


def load_queue_map(path: str) -> dict[str, str | list[str]]:
    """Load a queue-to-ASG mapping from a JSON file.

    The file maps HySDS job queue names to CE/ASG tag names (after prefix strip).
    Values can be a single string or a list (multiple ASGs per queue).
    Example:
        {
            "smap-pge-radiometer": ["smap-pge-radiometer-workflow", "smap-pge-radiometer-spot"],
            "smap-ingest-file": "smap-bulk-ingest-file"
        }

    Returns:
        Dict mapping job_queue_name -> ce_asg_name(s)
    """
    with open(path) as f:
        mapping = json.load(f)
    if not isinstance(mapping, dict):
        raise ValueError(f"Queue map file must contain a JSON object, got {type(mapping).__name__}")
    logger.info("Loaded queue map with %d entries from %s", len(mapping), path)
    return mapping


def apply_queue_map(
    ce_costs: dict[str, float],
    job_queues: set[str],
    queue_map: dict[str, str | list[str]] | None = None,
) -> tuple[dict[str, float], dict[str, float]]:
    """Match CE queue names to job queue names using an optional mapping.

    Matching rules (in order for each CE name):
    1. Inverted queue_map: if the CE name is a value in queue_map, map to the corresponding key
    2. Exact match: CE name is in job_queues

    Unmapped CE names that match a job queue exactly need no config entry.

    Args:
        ce_costs: Dict mapping CE queue name (after prefix strip) -> cost
        job_queues: Set of job queue names from Mozart
        queue_map: Optional dict mapping job_queue -> ce_asg_name or list of ce_asg_names

    Returns:
        (matched, unmatched) where matched is keyed by job queue name
    """
    # Invert: ce_name -> job_queue
    ce_to_queue: dict[str, str] = {}
    if queue_map:
        for job_q, ce_names in queue_map.items():
            if isinstance(ce_names, list):
                for ce_name in ce_names:
                    ce_to_queue[ce_name] = job_q
            else:
                ce_to_queue[ce_names] = job_q

    matched: dict[str, float] = {}
    unmatched: dict[str, float] = {}

    for ce_name, cost in ce_costs.items():
        if ce_name in ce_to_queue:
            job_q = ce_to_queue[ce_name]
            if job_q in job_queues:
                matched[job_q] = matched.get(job_q, 0) + cost
            else:
                logger.warning(
                    "Queue map maps CE '%s' -> job queue '%s', but no jobs found for that queue",
                    ce_name, job_q,
                )
                unmatched[ce_name] = cost
        elif ce_name in job_queues:
            matched[ce_name] = matched.get(ce_name, 0) + cost
        else:
            unmatched[ce_name] = cost

    return matched, unmatched


def safe_get(d: dict, *keys, default=None):
    """Safely traverse nested dict keys."""
    current = d
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
    return current
