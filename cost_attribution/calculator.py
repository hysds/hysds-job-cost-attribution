"""Cost calculation for individual jobs and queue overhead."""

import logging
from typing import Any

from .config import (
    AttributionConfig,
    S3_STORAGE_RATE_PER_GB_MONTH,
    S3_PUT_REQUEST_RATE_PER_1000,
    DAYS_PER_MONTH,
)
from .cost_explorer import get_instance_daily_cost
from .utils import safe_get, compute_s3_cost

logger = logging.getLogger(__name__)

SECONDS_PER_DAY = 86400


def calculate_job_cost(
    job: dict[str, Any],
    ec2_costs: dict[str, float],
    ebs_costs_by_queue: dict[str, float],
    queue_total_job_seconds: dict[str, float],
    config: AttributionConfig,
    pricing_rates: dict[str, float] | None = None,
    spot_types: set[str] | None = None,
    instance_counts: dict[str, int] | None = None,
    queue_costs: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Calculate cost components for a single job.

    Two allocation paths:
    1. CE tag-grouped (queue_costs provided): proportional allocation from per-queue CE costs.
       ec2_costs, ebs_costs_by_queue, pricing_rates are unused in this path.
    2. Per-instance (queue_costs is None): uses pricing_rates for hourly estimation.

    Args:
        job: Job document from OpenSearch
        ec2_costs: Dict mapping instance_type -> total daily cost (from CE type grouping)
        ebs_costs_by_queue: Dict mapping queue -> total EBS cost
        queue_total_job_seconds: Dict mapping queue -> total job duration in seconds
        config: Attribution configuration
        pricing_rates: Optional dict of pricing rates
        spot_types: Optional set of spot instance types
        instance_counts: Dict mapping instance_type -> number of instances (for per-instance split)
        queue_costs: Optional dict mapping queue -> CE tag-grouped costs (proportional allocation path)

    Returns a dict with cost breakdown:
        ec2_compute, ebs_storage, s3_output, total,
        ec2_hourly_rate, ec2_pricing_source, output_bytes
    """
    instance_id = job.get("ec2_instance_id", "")
    instance_type = job.get("ec2_instance_type", "")
    duration = safe_get(job, "job", "job_info", "duration", default=0) or 0
    queue = safe_get(job, "job", "job_info", "job_queue", default="unknown")

    if queue_costs is not None:
        # --- Proportional allocation from CE tag-grouped costs ---
        total_seconds = queue_total_job_seconds.get(queue, 0.0)
        queue_ce_cost = queue_costs.get(queue, 0.0)
        if total_seconds > 0 and duration > 0:
            job_share = duration / total_seconds
            infra_cost = queue_ce_cost * job_share
            ec2_hourly_rate = queue_ce_cost / total_seconds * 3600
        else:
            infra_cost = 0.0
            ec2_hourly_rate = 0.0
        pricing_source = "cost_explorer_tag"
        # EBS is already included in queue_costs — no separate allocation
        ebs_storage = 0.0
        ec2_compute = infra_cost
    else:
        # --- Per-instance pricing path (hourly estimator) ---
        if instance_id and duration > 0:
            daily_cost, pricing_source = get_instance_daily_cost(
                instance_id, instance_type, ec2_costs,
                pricing_rates or {}, config.use_fallback_pricing,
                spot_types=spot_types,
                instance_counts=instance_counts,
            )
            cost_per_second = daily_cost / SECONDS_PER_DAY
            ec2_compute = duration * cost_per_second
            ec2_hourly_rate = daily_cost / 24
        else:
            ec2_compute = 0.0
            ec2_hourly_rate = 0.0
            pricing_source = "fallback"

        # --- EBS Storage ---
        queue_ebs = ebs_costs_by_queue.get(queue, 0.0)
        total_seconds = queue_total_job_seconds.get(queue, 0.0)
        if total_seconds > 0 and duration > 0:
            ebs_storage = queue_ebs * (duration / total_seconds)
        else:
            ebs_storage = 0.0

    # --- S3 Output ---
    products = safe_get(job, "job", "job_info", "metrics", "products_staged") or []
    output_bytes = sum(
        p.get("disk_usage", 0) or 0 for p in products
    )
    num_products = len(products)
    s3_output = compute_s3_cost(output_bytes, num_products)

    total = ec2_compute + ebs_storage + s3_output

    return {
        "ec2_compute": round(ec2_compute, 6),
        "ebs_storage": round(ebs_storage, 6),
        "s3_output": round(s3_output, 6),
        "total": round(total, 6),
        "ec2_hourly_rate": round(ec2_hourly_rate, 6),
        "ec2_pricing_source": pricing_source,
        "output_bytes": output_bytes,
    }


def calculate_queue_overhead(
    queue: str,
    total_instance_cost: float,
    total_job_ec2_cost: float,
    total_ebs_cost: float,
    total_job_ebs_cost: float,
    total_instance_hours: float,
    total_job_hours: float,
    instance_count: int,
    job_count: int,
) -> dict[str, Any]:
    """Calculate overhead costs for a queue.

    Overhead = total cost - sum of attributed job costs.
    Covers ASG spin-up/down time and inter-job gaps.

    Note: When using CE tag-grouped costs, total_ebs_cost is 0 because EBS is
    bundled into the per-queue CE total. total_instance_hours and instance_count
    are placeholders pending instance-level CE data integration.
    """
    ec2_idle = max(0.0, total_instance_cost - total_job_ec2_cost)
    ebs_idle = max(0.0, total_ebs_cost - total_job_ebs_cost)
    total_overhead = ec2_idle + ebs_idle

    total_hours = total_instance_hours if total_instance_hours > 0 else 1
    idle_pct = ((total_hours - total_job_hours) / total_hours) * 100

    return {
        "queue": queue,
        "cost": {
            "total": round(total_overhead, 6),
            "ec2_idle": round(ec2_idle, 6),
            "ebs_idle": round(ebs_idle, 6),
        },
        "overhead_details": {
            "total_instance_hours": round(total_instance_hours, 4),
            "total_job_hours": round(total_job_hours, 4),
            "idle_pct": round(max(0.0, idle_pct), 2),
            "instance_count": instance_count,
            "job_count": job_count,
        },
    }


def compute_queue_totals(
    jobs: list[dict[str, Any]],
    job_costs: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    """Aggregate job costs and durations by queue.

    Returns dict[queue] -> {total_ec2, total_ebs, total_seconds, job_count}
    """
    totals: dict[str, dict[str, float]] = {}

    for job, cost in zip(jobs, job_costs):
        queue = safe_get(job, "job", "job_info", "job_queue", default="unknown")
        duration = safe_get(job, "job", "job_info", "duration", default=0) or 0

        if queue not in totals:
            totals[queue] = {
                "total_ec2": 0.0,
                "total_ebs": 0.0,
                "total_seconds": 0.0,
                "job_count": 0,
            }

        totals[queue]["total_ec2"] += cost["ec2_compute"]
        totals[queue]["total_ebs"] += cost["ebs_storage"]
        totals[queue]["total_seconds"] += duration
        totals[queue]["job_count"] += 1

    return totals
