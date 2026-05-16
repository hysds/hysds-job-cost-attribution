"""Configuration for the cost attribution batch process."""

from dataclasses import dataclass, field
from typing import Optional

# S3 pricing constants (us-west-2)
S3_STORAGE_RATE_PER_GB_MONTH = 0.023
S3_PUT_REQUEST_RATE_PER_1000 = 0.005
DAYS_PER_MONTH = 30

# OpenSearch hosts
MOZART_HOST = "localhost"
MOZART_PORT = 9300
METRICS_HOST = "localhost"
METRICS_PORT = 9400

# Redis settings
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_KEY = "cost-attribution"

# Job index pattern
JOB_INDEX_PATTERN = "job_status-*"
COST_INDEX_PREFIX = "job-cost-attribution"

# Terminal job statuses that should be costed
TERMINAL_STATUSES = [
    "job-completed",
    "job-failed",
    "job-deduped",
    "job-revoked",
]


@dataclass
class AttributionConfig:
    """Runtime configuration for a cost attribution run."""

    # Attribution window
    window_start: str  # ISO 8601
    window_end: str  # ISO 8601
    run_id: str  # Unique ID for this run (for dedup)

    # OpenSearch
    mozart_host: str = MOZART_HOST
    mozart_port: int = MOZART_PORT
    metrics_host: str = METRICS_HOST
    metrics_port: int = METRICS_PORT
    job_index: str = JOB_INDEX_PATTERN

    # Redis
    redis_host: str = REDIS_HOST
    redis_port: int = REDIS_PORT
    redis_password: Optional[str] = None
    redis_key: str = REDIS_KEY

    # AWS
    aws_region: str = "us-west-2"

    # ASG prefix to strip when mapping to queue names
    asg_prefix: str = "maap-dps"

    # Batch size for Redis pushes
    redis_batch_size: int = 500

    # Publish mode: "redis" (default, via Logstash) or "direct" (bulk to metrics OpenSearch)
    publish_mode: str = "redis"
    bulk_batch_size: int = 500

    # Cost Explorer
    use_fallback_pricing: bool = False  # Force fallback pricing
    cost_metric: str = "AmortizedCost"  # UnblendedCost, AmortizedCost, or BlendedCost

    @property
    def mozart_url(self) -> str:
        return f"http://{self.mozart_host}:{self.mozart_port}"

    @property
    def metrics_url(self) -> str:
        return f"http://{self.metrics_host}:{self.metrics_port}"
