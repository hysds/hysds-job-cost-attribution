"""Utility functions for cost attribution."""

import re
from datetime import datetime, timezone


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


def safe_get(d: dict, *keys, default=None):
    """Safely traverse nested dict keys."""
    current = d
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
    return current
