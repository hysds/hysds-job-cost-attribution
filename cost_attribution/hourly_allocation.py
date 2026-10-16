"""Hourly cost allocation based on running instance-seconds.

Each queue-hour has a base rate (cost of the instance time a job used):

    base rate = CE cost of the queue's Name tag in that hour
                / max(running instance-seconds, job-seconds) in that hour

Instances run one job at a time, so cost the base rate doesn't cover is idle
time (scale-up/down, gaps between jobs). With idle_to_jobs (the default) that
idle cost is split among the jobs that ran in the same hour, per second:

    rate = CE cost in that hour / job-seconds in that hour

so a queue-hour's full cost lands on the jobs that ran in it. Only hours in
which no job ran remain as queue overhead. With idle_to_jobs=False the base
rate is charged and all idle time is queue overhead. Retried attempts are
charged for their own hours, like any other job.

Hours outside CE's 14-day HOURLY retention use the same formulas with daily
CE totals (daily cost / daily running instance-seconds or job-seconds).
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from .utils import asg_name_to_queue, iso_to_datetime, job_run_interval, safe_get

logger = logging.getLogger(__name__)

HOUR = timedelta(hours=1)
# Job-seconds may exceed CE running seconds by this much before it is reported
# (CE usage is metered per second but jobs record their own start/end times).
OCCUPANCY_TOLERANCE_SECONDS = 60

SOURCE_HOURLY = "ce_hourly_instance_seconds"
SOURCE_DAILY = "ce_daily_instance_seconds"
SOURCE_MISSING = "no_ce_data"


@dataclass
class QueueDay:
    """Cost and usage totals for one queue on one day."""

    cost: float = 0.0
    job_cost: float = 0.0
    idle_to_jobs: float = 0.0  # part of job_cost that is redistributed idle time
    instance_seconds: float = 0.0
    job_seconds: float = 0.0
    instances: set = field(default_factory=set)
    job_ids: set = field(default_factory=set)


@dataclass
class HourlyAllocation:
    """Result of allocate_hourly()."""

    # Per job (same order as input): ec2_compute, ec2_idle_share, ec2_hourly_rate,
    # pricing_source
    job_costs: list[dict[str, Any]]
    # (queue, day) -> totals, only for report days
    queue_days: dict[tuple[str, str], QueueDay]
    # day -> CE cost of Name tags that are not job queues (pcm-*, etc.)
    infra_by_day: dict[str, float]
    # Diagnostics
    over_occupied_hours: int = 0
    max_occupancy_ratio: float = 0.0
    over_occupied_queues: dict[str, int] = field(default_factory=dict)
    unpriced_job_seconds: float = 0.0


def _ce_to_queue(queue_map: dict[str, str | list[str]] | None) -> dict[str, str]:
    """Invert a job_queue -> CE name(s) map into CE name -> job_queue."""
    inverted: dict[str, str] = {}
    for job_q, ce_names in (queue_map or {}).items():
        for ce_name in ce_names if isinstance(ce_names, list) else [ce_names]:
            inverted[ce_name] = job_q
    return inverted


def map_usage_to_queues(
    period_usage: dict[str, dict[str, float]],
    asg_prefix: str,
    ce_to_queue: dict[str, str],
    job_queues: set[str],
) -> tuple[dict[str, dict[str, float]], dict[str, float]]:
    """Split one period's per-tag usage into job queues and non-queue infra.

    A tag counts as a queue if it maps to a queue that has jobs anywhere in
    the run (or is in the queue map), so a worker ASG that sat idle all day is
    still charged to its queue as overhead rather than dropped as infra.
    """
    queues: dict[str, dict[str, float]] = {}
    infra: dict[str, float] = {}
    for name, usage in period_usage.items():
        stripped = asg_name_to_queue(name, asg_prefix)
        queue = ce_to_queue.get(stripped, stripped)
        if queue in job_queues:
            entry = queues.setdefault(queue, {"cost": 0.0, "instance_seconds": 0.0})
            entry["cost"] += usage["cost"]
            entry["instance_seconds"] += usage["instance_seconds"]
        else:
            infra[stripped] = infra.get(stripped, 0.0) + usage["cost"]
    return queues, infra


def split_by_hour(start: datetime, end: datetime) -> list[tuple[datetime, float]]:
    """Split [start, end) into (hour start, seconds in that hour) pieces."""
    pieces = []
    hour = start.replace(minute=0, second=0, microsecond=0)
    while hour < end:
        next_hour = hour + HOUR
        seconds = (min(end, next_hour) - max(start, hour)).total_seconds()
        if seconds > 0:
            pieces.append((hour, seconds))
        hour = next_hour
    return pieces


def _day(hour: datetime) -> str:
    return hour.strftime("%Y-%m-%d")


def allocate_hourly(
    jobs: list[dict[str, Any]],
    hourly_usage: dict[str, dict[str, dict[str, float]]],
    daily_usage: dict[str, dict[str, dict[str, float]]],
    report_days: set[str],
    asg_prefix: str = "",
    queue_map: dict[str, str | list[str]] | None = None,
    idle_to_jobs: bool = True,
    occupancy_complete_from: datetime | None = None,
) -> HourlyAllocation:
    """Allocate CE queue cost to jobs by the seconds they ran in each hour.

    Args:
        jobs: All jobs that may overlap the report days, including jobs that end
            after the last report day (their in-range hours count toward
            occupancy even though they get no job document).
        hourly_usage: fetch_queue_usage(..., "HOURLY") output
        daily_usage: fetch_queue_usage(..., "DAILY") output, used for hours
            not covered by hourly_usage
        report_days: Days (YYYY-MM-DD) to produce queue/infra totals for
        asg_prefix: Prefix stripped from Name tags to get queue names
        queue_map: Optional job_queue -> CE name(s) mapping
        idle_to_jobs: Split each hour's idle cost among the jobs that ran in it
            (otherwise idle cost is queue overhead)
        occupancy_complete_from: Earliest hour for which `jobs` includes every
            job that ran (i.e. the job fetch window start). Idle cost is only
            redistributed from that hour on; earlier hours would spread a full
            hour's cost over the few jobs we happen to know about.
    """
    job_queues = {
        safe_get(job, "job", "job_info", "job_queue", default="unknown") for job in jobs
    } | set(queue_map or {})
    ce_to_queue = _ce_to_queue(queue_map)

    infra_by_day: dict[str, float] = defaultdict(float)

    hourly_q: dict[datetime, dict[str, dict[str, float]]] = {}
    for period, usage in hourly_usage.items():
        hour = iso_to_datetime(period)
        hourly_q[hour], infra = map_usage_to_queues(usage, asg_prefix, ce_to_queue, job_queues)
        if _day(hour) in report_days:
            infra_by_day[_day(hour)] += sum(infra.values())
    hourly_days = {_day(h) for h in hourly_q}

    daily_q: dict[str, dict[str, dict[str, float]]] = {}
    for day, usage in daily_usage.items():
        daily_q[day], infra = map_usage_to_queues(usage, asg_prefix, ce_to_queue, job_queues)
        if day in report_days and day not in hourly_days:
            infra_by_day[day] += sum(infra.values())

    # Occupancy: job-seconds per queue-hour and queue-day
    job_pieces: list[list[tuple[datetime, float]]] = []
    occupancy_hour: dict[tuple[str, datetime], float] = defaultdict(float)
    occupancy_day: dict[tuple[str, str], float] = defaultdict(float)
    for job in jobs:
        queue = safe_get(job, "job", "job_info", "job_queue", default="unknown")
        interval = job_run_interval(job)
        pieces = split_by_hour(*interval) if interval else []
        job_pieces.append(pieces)
        for hour, seconds in pieces:
            occupancy_hour[(queue, hour)] += seconds
            occupancy_day[(queue, _day(hour))] += seconds

    rates: dict[tuple[str, datetime], tuple[float, float, str]] = {}

    def occupancy_complete(period_start: datetime) -> bool:
        return occupancy_complete_from is None or period_start >= occupancy_complete_from

    def rate_for(queue: str, hour: datetime) -> tuple[float, float, str]:
        """Return (rate charged, base rate, source) for a queue-hour."""
        key = (queue, hour)
        if key not in rates:
            if hour in hourly_q:
                usage = hourly_q[hour].get(queue)
                occupied = occupancy_hour[key]
                period_start = hour
                source = SOURCE_HOURLY
            elif _day(hour) in daily_q and _day(hour) not in hourly_days:
                usage = daily_q[_day(hour)].get(queue)
                occupied = occupancy_day[(queue, _day(hour))]
                period_start = hour.replace(hour=0)
                source = SOURCE_DAILY
            else:
                usage, occupied, period_start, source = None, 0.0, hour, SOURCE_MISSING
            base = rate = 0.0
            if usage:
                denominator = max(usage["instance_seconds"], occupied)
                base = rate = usage["cost"] / denominator if denominator > 0 else 0.0
                if idle_to_jobs and occupied > 0 and occupancy_complete(period_start):
                    rate = usage["cost"] / occupied
            rates[key] = (rate, base, source)
        return rates[key]

    # Per-job cost
    job_costs: list[dict[str, Any]] = []
    unpriced_seconds = 0.0
    for job, pieces in zip(jobs, job_pieces):
        queue = safe_get(job, "job", "job_info", "job_queue", default="unknown")
        compute = 0.0
        idle_share = 0.0
        seconds_total = 0.0
        sources = set()
        for hour, seconds in pieces:
            rate, base, source = rate_for(queue, hour)
            compute += rate * seconds
            idle_share += (rate - base) * seconds
            seconds_total += seconds
            sources.add(source)
            if rate == 0.0 and _day(hour) in report_days:
                unpriced_seconds += seconds
        job_costs.append({
            "ec2_compute": compute,
            "ec2_idle_share": idle_share,
            "ec2_hourly_rate": compute / seconds_total * 3600 if seconds_total else 0.0,
            "pricing_source": sources.pop() if len(sources) == 1 else ("mixed" if sources else "fallback"),
        })

    # Queue-day totals for report days
    queue_days: dict[tuple[str, str], QueueDay] = defaultdict(QueueDay)
    over_occupied: dict[str, int] = defaultdict(int)
    max_ratio = 0.0

    for hour, queues in hourly_q.items():
        if _day(hour) not in report_days:
            continue
        for queue, usage in queues.items():
            qd = queue_days[(queue, _day(hour))]
            qd.cost += usage["cost"]
            qd.instance_seconds += usage["instance_seconds"]
    for day, queues in daily_q.items():
        if day not in report_days or day in hourly_days:
            continue
        for queue, usage in queues.items():
            qd = queue_days[(queue, day)]
            qd.cost += usage["cost"]
            qd.instance_seconds += usage["instance_seconds"]

    for (queue, hour), occupied in occupancy_hour.items():
        if _day(hour) not in report_days:
            continue
        rate, base, source = rate_for(queue, hour)
        qd = queue_days[(queue, _day(hour))]
        qd.job_seconds += occupied
        qd.job_cost += rate * occupied
        qd.idle_to_jobs += (rate - base) * occupied
        if source == SOURCE_HOURLY:
            capacity = hourly_q[hour].get(queue, {}).get("instance_seconds", 0.0)
            if occupied > capacity + OCCUPANCY_TOLERANCE_SECONDS:
                over_occupied[queue] += 1
                if capacity > 0:
                    max_ratio = max(max_ratio, occupied / capacity)

    for job, pieces in zip(jobs, job_pieces):
        queue = safe_get(job, "job", "job_info", "job_queue", default="unknown")
        for day in {_day(hour) for hour, _ in pieces} & report_days:
            qd = queue_days[(queue, day)]
            qd.job_ids.add(job.get("payload_id", ""))
            if job.get("ec2_instance_id"):
                qd.instances.add(job["ec2_instance_id"])

    return HourlyAllocation(
        job_costs=job_costs,
        queue_days=dict(queue_days),
        infra_by_day=dict(infra_by_day),
        over_occupied_hours=sum(over_occupied.values()),
        over_occupied_queues=dict(over_occupied),
        max_occupancy_ratio=max_ratio,
        unpriced_job_seconds=unpriced_seconds,
    )
