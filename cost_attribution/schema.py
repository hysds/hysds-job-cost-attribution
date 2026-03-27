"""OpenSearch index template and document builders for cost attribution."""

from datetime import datetime, timezone
from typing import Any

from .config import COST_INDEX_PREFIX
from .utils import parse_job_type, safe_get, format_date_index


INDEX_TEMPLATE = {
    "index_patterns": [f"{COST_INDEX_PREFIX}-*"],
    "template": {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
        },
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "doc_type": {"type": "keyword"},
                "attribution_window": {
                    "properties": {
                        "start": {"type": "date"},
                        "end": {"type": "date"},
                        "run_id": {"type": "keyword"},
                    }
                },
                "job_id": {"type": "keyword"},
                "job_type": {"type": "keyword"},
                "job_version": {"type": "keyword"},
                "algorithm": {"type": "keyword"},
                "container": {"type": "keyword"},
                "username": {"type": "keyword"},
                "queue": {"type": "keyword"},
                "tag": {"type": "keyword"},
                "status": {"type": "keyword"},
                "instance_id": {"type": "keyword"},
                "instance_type": {"type": "keyword"},
                "availability_zone": {"type": "keyword"},
                "is_retry_attempt": {"type": "boolean"},
                "timing": {
                    "properties": {
                        "queued": {"type": "date"},
                        "started": {"type": "date"},
                        "ended": {"type": "date"},
                        "duration_seconds": {"type": "double"},
                        "queue_wait_seconds": {"type": "double"},
                    }
                },
                "cost": {
                    "properties": {
                        "total": {"type": "double"},
                        "ec2_compute": {"type": "double"},
                        "ebs_storage": {"type": "double"},
                        "s3_output": {"type": "double"},
                        "ec2_idle": {"type": "double"},
                        "ebs_idle": {"type": "double"},
                    }
                },
                "cost_inputs": {
                    "properties": {
                        "ec2_hourly_rate": {"type": "double"},
                        "ec2_pricing_source": {"type": "keyword"},
                        "output_bytes": {"type": "long"},
                        "pricing_source": {"type": "keyword"},
                        "cost_metric": {"type": "keyword"},
                        "last_updated": {"type": "date"},
                    }
                },
                "resource_usage": {
                    "properties": {
                        "cpu_total_ns": {"type": "long"},
                        "memory_max_bytes": {"type": "long"},
                    }
                },
                "overhead_details": {
                    "properties": {
                        "total_instance_hours": {"type": "double"},
                        "total_job_hours": {"type": "double"},
                        "idle_pct": {"type": "double"},
                        "instance_count": {"type": "integer"},
                        "job_count": {"type": "integer"},
                    }
                },
            }
        },
    },
}


def build_job_cost_document(
    job: dict[str, Any],
    cost: dict[str, Any],
    config_window_start: str,
    config_window_end: str,
    run_id: str,
    cost_metric: str = "AmortizedCost",
) -> dict[str, Any]:
    """Build a job_cost document for OpenSearch.

    Args:
        job: Raw job document from Mozart
        cost: Cost breakdown from calculator
        config_window_start: Attribution window start (ISO 8601)
        config_window_end: Attribution window end (ISO 8601)
        run_id: Unique run identifier for dedup
    """
    job_type = job.get("type", "")
    algorithm, version = parse_job_type(job_type)

    time_queued = safe_get(job, "job", "job_info", "time_queued")
    time_start = safe_get(job, "job", "job_info", "time_start")
    time_end = safe_get(job, "job", "job_info", "time_end")
    duration = safe_get(job, "job", "job_info", "duration", default=0) or 0

    # Calculate queue wait time
    queue_wait = 0.0
    if time_queued and time_start:
        try:
            from .utils import iso_to_datetime
            t_queued = iso_to_datetime(time_queued)
            t_start = iso_to_datetime(time_start)
            queue_wait = (t_start - t_queued).total_seconds()
        except (ValueError, TypeError):
            pass

    # Resource usage from cgroups
    resource = job.get("resource_usage", {}) or {}

    # Build the target index name from window start date
    window_dt = datetime.fromisoformat(config_window_start.replace("Z", "+00:00"))
    index_name = f"{COST_INDEX_PREFIX}-{format_date_index(window_dt)}"

    return {
        "@index": index_name,
        "doc_id": job.get("payload_id", ""),
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "doc_type": "job_cost",
        "attribution_window": {
            "start": config_window_start,
            "end": config_window_end,
            "run_id": run_id,
        },
        "job_id": job.get("payload_id", ""),
        "job_type": job_type,
        "job_version": version,
        "algorithm": algorithm,
        "container": job.get("container_image_name", ""),
        "username": safe_get(job, "job", "username", default=""),
        "queue": safe_get(job, "job", "job_info", "job_queue", default=""),
        "tag": safe_get(job, "job", "tag", default=""),
        "status": job.get("status", ""),
        "instance_id": job.get("ec2_instance_id", ""),
        "instance_type": job.get("ec2_instance_type", ""),
        "availability_zone": job.get("ec2_placement_availability_zone", ""),
        "is_retry_attempt": job.get("is_retry_attempt", False),
        "timing": {
            "queued": time_queued,
            "started": time_start,
            "ended": time_end,
            "duration_seconds": int(duration),
            "queue_wait_seconds": int(queue_wait),
        },
        "cost": {
            "total": cost["total"],
            "ec2_compute": cost["ec2_compute"],
            "ebs_storage": cost["ebs_storage"],
            "s3_output": cost["s3_output"],
        },
        "cost_inputs": {
            "ec2_hourly_rate": cost["ec2_hourly_rate"],
            "ec2_pricing_source": cost["ec2_pricing_source"],
            "output_bytes": cost["output_bytes"],
            "pricing_source": cost["ec2_pricing_source"],
            "cost_metric": cost_metric,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        },
        "resource_usage": {
            "cpu_total_ns": resource.get("cpu_total_ns"),
            "memory_max_bytes": resource.get("memory_max_bytes"),
        },
    }


def build_queue_overhead_document(
    overhead: dict[str, Any],
    config_window_start: str,
    config_window_end: str,
    run_id: str,
    cost_metric: str = "AmortizedCost",
) -> dict[str, Any]:
    """Build a queue_overhead document for OpenSearch."""
    window_dt = datetime.fromisoformat(config_window_start.replace("Z", "+00:00"))
    index_name = f"{COST_INDEX_PREFIX}-{format_date_index(window_dt)}"

    return {
        "@index": index_name,
        "doc_id": f"overhead-{overhead['queue']}-{config_window_start[:10]}",
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "doc_type": "queue_overhead",
        "attribution_window": {
            "start": config_window_start,
            "end": config_window_end,
            "run_id": run_id,
        },
        "queue": overhead["queue"],
        "cost": overhead["cost"],
        "cost_inputs": {
            "cost_metric": cost_metric,
        },
        "overhead_details": overhead["overhead_details"],
    }
