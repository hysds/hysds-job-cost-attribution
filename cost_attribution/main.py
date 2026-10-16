"""CLI entry point for the cost attribution batch process."""
import json
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import click

from .config import AttributionConfig, CE_HOURLY_RETENTION_DAYS, HOURLY_JOB_PAD_DAYS
from .cost_explorer import (
    create_ce_client,
    fetch_queue_costs,
    fetch_queue_costs_by_day,
    fetch_queue_usage,
    fetch_ondemand_hourly_rates,
    fetch_spot_hourly_rates,
)
from .calculator import (
    build_cost_breakdown,
    calculate_job_cost,
    calculate_queue_overhead,
    compute_queue_totals,
)
from .hourly_allocation import allocate_hourly
from .mozart_client import fetch_all_jobs, fetch_metrics_extra_attempts
from .redis_publisher import publish_documents, check_redis_connection
from .metrics_publisher import bulk_index_documents, check_metrics_connection
from .schema import build_job_cost_document, build_queue_overhead_document
from .utils import (
    asg_name_to_queue, apply_queue_map, filter_skipped_job_types, job_billed_seconds,
    load_queue_map, safe_get,
)

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool) -> None:
    """Configure logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    if verbose:
        # Only enable DEBUG for our modules, not boto3/urllib3/opensearch
        logging.getLogger("cost_attribution").setLevel(logging.DEBUG)
        logging.getLogger("__main__").setLevel(logging.DEBUG)


def build_config(
    date: str,
    redis_password: str | None,
    asg_prefix: str,
    region: str,
    use_fallback: bool,
    cost_metric: str = "AmortizedCost",
    publish_mode: str = "redis",
) -> AttributionConfig:
    """Build AttributionConfig for the given date."""
    target_date = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    window_start = target_date.strftime("%Y-%m-%dT00:00:00Z")
    window_end = target_date.strftime("%Y-%m-%dT23:59:59Z")
    run_id = f"{date}-{uuid.uuid4().hex[:8]}"

    return AttributionConfig(
        window_start=window_start,
        window_end=window_end,
        run_id=run_id,
        redis_password=redis_password,
        asg_prefix=asg_prefix,
        aws_region=region,
        use_fallback_pricing=use_fallback,
        cost_metric=cost_metric,
        publish_mode=publish_mode,
    )


def preflight_publish(config: AttributionConfig) -> None:
    """Verify the configured publish target is reachable."""
    if config.publish_mode == "direct":
        if not check_metrics_connection(config):
            raise click.ClickException(
                "Cannot connect to metrics OpenSearch. Check --metrics settings."
            )
    else:
        if not check_redis_connection(config):
            raise click.ClickException("Cannot connect to Redis. Check connection settings.")


def publish(documents: list[dict], config: AttributionConfig) -> int:
    """Dispatch to the configured publisher (redis or direct bulk)."""
    if config.publish_mode == "direct":
        return bulk_index_documents(documents, config)
    return publish_documents(documents, config)


def write_documents_json(documents: list[dict], path: str) -> int:
    """Write documents as JSON Lines (one document per line), exactly as they would be published."""
    with open(path, "w") as f:
        for doc in documents:
            f.write(json.dumps(doc, default=str) + "\n")
    logger.info("Wrote %d documents to %s", len(documents), path)
    return len(documents)


def run_hourly_estimator(
    region: str,
    asg_prefix: str,
    dry_run: bool,
    verbose: bool,
    skip_job_types: tuple[str, ...] = (),
    cost_metric: str = "AmortizedCost",
    publish_mode: str = "redis",
    output_json: str | None = None,
) -> None:
    """Hourly estimator: cost recent terminal jobs that lack cost documents.

    Queries Mozart for terminal jobs in the last 48h, checks which already
    have cost docs in OpenSearch, and estimates costs for the remainder
    using spot/on-demand pricing.
    """
    from opensearchpy import OpenSearch
    from .config import COST_INDEX_PREFIX, TERMINAL_STATUSES
    from .schema import build_job_cost_document
    from .utils import safe_get

    now = datetime.now(timezone.utc)
    window_start = (now - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%SZ")
    window_end = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    run_id = f"estimate-{now.strftime('%Y%m%d%H%M')}-{uuid.uuid4().hex[:8]}"

    logger.info("Hourly estimator: scanning %s → %s", window_start, window_end)

    # Build a config for the 48h window
    config = AttributionConfig(
        window_start=window_start,
        window_end=window_end,
        run_id=run_id,
        asg_prefix=asg_prefix,
        aws_region=region,
        use_fallback_pricing=True,  # No CE in estimator mode
        cost_metric=cost_metric,
        publish_mode=publish_mode,
    )

    if not dry_run:
        preflight_publish(config)

    # Fetch terminal jobs from last 48h
    jobs = fetch_all_jobs(config)
    if not jobs:
        logger.info("No terminal jobs in last 48h. Nothing to estimate.")
        click.echo("No recent terminal jobs found.")
        return

    logger.info("Found %d terminal jobs in last 48h", len(jobs))

    # Filter out skipped job types
    jobs = filter_skipped_job_types(jobs, skip_job_types)

    # Query cost index to find jobs that already have cost docs
    cost_client = OpenSearch(
        hosts=[{"host": config.mozart_host, "port": config.mozart_port}],
        use_ssl=False,
        verify_certs=False,
        timeout=30,
    )

    job_ids = [j.get("payload_id", "") for j in jobs if j.get("payload_id")]
    existing_ids: set[str] = set()

    # Check in batches of 1000
    for i in range(0, len(job_ids), 1000):
        batch = job_ids[i : i + 1000]
        try:
            result = cost_client.search(
                index=f"{COST_INDEX_PREFIX}-*",
                body={
                    "query": {"terms": {"job_id": batch}},
                    "_source": ["job_id"],
                    "size": len(batch),
                },
            )
            for hit in result["hits"]["hits"]:
                existing_ids.add(hit["_source"]["job_id"])
        except Exception as e:
            logger.warning("Cost index query failed: %s", e)

    # Filter to jobs without cost docs
    jobs_to_estimate = [
        j for j in jobs
        if j.get("payload_id", "") and j.get("payload_id", "") not in existing_ids
    ]
    logger.info(
        "%d jobs already have cost docs, %d need estimates",
        len(existing_ids), len(jobs_to_estimate),
    )

    if not jobs_to_estimate:
        click.echo("All recent jobs already have cost estimates.")
        return

    # Fetch pricing rates (spot first, on-demand fallback)
    instance_types = list({
        j.get("ec2_instance_type", "")
        for j in jobs_to_estimate
        if j.get("ec2_instance_type")
    })
    availability_zones = list({
        j.get("ec2_placement_availability_zone", "")
        for j in jobs_to_estimate
        if j.get("ec2_placement_availability_zone")
    })

    pricing_rates = fetch_spot_hourly_rates(
        instance_types, availability_zones, region,
    )
    spot_types = set(pricing_rates.keys())
    missing_types = [t for t in instance_types if t not in pricing_rates]
    if missing_types:
        ondemand_rates = fetch_ondemand_hourly_rates(missing_types, region)
        pricing_rates.update(ondemand_rates)

    # Calculate costs (no CE data, no EBS allocation in estimator mode)
    from .calculator import calculate_job_cost

    documents = []
    for job in jobs_to_estimate:
        cost = calculate_job_cost(
            job,
            ec2_costs={},                    # Not used: estimator uses pricing_rates
            ebs_costs_by_queue={},           # Not used: no EBS allocation in estimator
            queue_total_job_seconds={},
            config=config,
            pricing_rates=pricing_rates,
            spot_types=spot_types,
        )
        doc = build_job_cost_document(
            job, cost, window_start, window_end, run_id,
            cost_metric=config.cost_metric,
        )
        documents.append(doc)

    total_cost = sum(
        d.get("cost", {}).get("total", 0) for d in documents
    )

    logger.info(
        "Estimated costs for %d jobs, total: $%.4f",
        len(documents), total_cost,
    )

    # Publish
    if output_json:
        write_documents_json(documents, output_json)
    if dry_run:
        logger.info("DRY RUN — skipping publish")
        for doc in documents[:3]:
            logger.debug(
                "Sample: job=%s cost=$%.6f source=%s",
                doc.get("job_id"),
                doc.get("cost", {}).get("total", 0),
                doc.get("cost_inputs", {}).get("pricing_source"),
            )
    else:
        published = publish(documents, config)
        logger.info("Published %d estimate documents (mode=%s)", published, config.publish_mode)

    click.echo(f"\nHourly Estimator Summary")
    click.echo(f"{'=' * 40}")
    click.echo(f"Cost metric:       {config.cost_metric}")
    click.echo(f"Jobs in last 48h:  {len(jobs)}")
    click.echo(f"Already costed:    {len(existing_ids)}")
    click.echo(f"Newly estimated:   {len(documents)}")
    click.echo(f"Estimated total:   ${total_cost:.4f}")
    click.echo(f"Run ID:            {run_id}")
    if output_json:
        click.echo(f"JSON output:       {output_json}")
    if dry_run:
        click.echo("Mode:              DRY RUN (not published)")
    else:
        click.echo("Mode:              PUBLISHED")


def run_date_range(
    start_date: str,
    end_date: str,
    redis_password: str | None,
    asg_prefix: str,
    region: str,
    use_fallback: bool,
    include_retries: bool,
    dry_run: bool,
    skip_job_types: tuple[str, ...] = (),
    cost_metric: str = "AmortizedCost",
    queue_map: dict[str, str] | None = None,
    publish_mode: str = "redis",
    output_json: str | None = None,
) -> None:
    """Backfill cost attribution across a date range with a single CE call.

    Fetches CE costs and Mozart jobs for the full range upfront, then
    processes each day individually, reusing the shared data.

    Args:
        start_date: Range start (YYYY-MM-DD, inclusive)
        end_date: Range end (YYYY-MM-DD, inclusive)
        redis_password: Redis password for publishing
        asg_prefix: ASG name prefix to strip when mapping to queue names
        region: AWS region for Cost Explorer
        use_fallback: Force fallback pricing (skip CE)
        dry_run: Calculate costs but skip publishing
    """
    logger.info("Starting date range backfill: %s to %s", start_date, end_date)

    # Validate dates
    try:
        dt_start = datetime.strptime(start_date, "%Y-%m-%d")
        dt_end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        raise click.ClickException(f"Invalid date format: {e}")

    if dt_start > dt_end:
        raise click.ClickException(
            f"Start date {start_date} is after end date {end_date}"
        )

    # CE end_date is exclusive, so add 1 day
    ce_end_exclusive = (dt_end + timedelta(days=1)).strftime("%Y-%m-%d")

    # Step 1: Check publisher connectivity (unless dry run)
    temp_config = build_config(start_date, redis_password, asg_prefix, region, use_fallback, cost_metric, publish_mode)
    if not dry_run:
        preflight_publish(temp_config)

    # Step 2: Fetch CE costs for full range in ONE call
    costs_by_day: dict[str, dict[str, float]] = {}
    if not use_fallback:
        ce_client = create_ce_client(temp_config)
        costs_by_day = fetch_queue_costs_by_day(
            ce_client, start_date, ce_end_exclusive, asg_prefix,
            cost_metric=temp_config.cost_metric,
        )
        logger.info(
            "Fetched CE costs for %d days in single API call", len(costs_by_day),
        )

    # Step 3: Fetch ALL jobs from Mozart for the full range at once
    range_config = AttributionConfig(
        window_start=f"{start_date}T00:00:00Z",
        window_end=f"{end_date}T23:59:59Z",
        run_id=f"range-{start_date}-to-{end_date}",
        redis_password=redis_password,
        asg_prefix=asg_prefix,
        aws_region=region,
        use_fallback_pricing=use_fallback,
        cost_metric=cost_metric,
        publish_mode=publish_mode,
    )
    all_jobs = fetch_all_jobs(range_config)
    logger.info("Fetched %d total jobs for full range", len(all_jobs))

    # Supplement with retry attempts from metrics
    if include_retries:
        extra = fetch_metrics_extra_attempts(range_config, all_jobs)
        if extra:
            all_jobs.extend(extra)
            logger.info("Added %d retry attempts, total jobs now: %d", len(extra), len(all_jobs))

    # Filter out skipped job types
    all_jobs = filter_skipped_job_types(all_jobs, skip_job_types)

    # Step 4: Group jobs by day using time_end
    jobs_by_day: dict[str, list] = defaultdict(list)
    skipped_no_date = 0
    for job in all_jobs:
        time_end = safe_get(job, "job", "job_info", "time_end")
        if not time_end:
            skipped_no_date += 1
            continue
        # Extract date portion (YYYY-MM-DD) from ISO timestamp
        day_key = str(time_end)[:10]
        jobs_by_day[day_key].append(job)

    if skipped_no_date:
        logger.warning("Skipped %d jobs with no time_end", skipped_no_date)

    # Step 5: Process each day in the range
    all_documents: list[dict] = []
    grand_total_job_cost = 0.0
    grand_total_overhead = 0.0
    grand_total_infra = 0.0
    days_processed = 0
    days_skipped_no_jobs = 0
    days_skipped_no_ce = 0

    current = dt_start
    while current <= dt_end:
        day_str = current.strftime("%Y-%m-%d")
        current += timedelta(days=1)

        day_jobs = jobs_by_day.get(day_str, [])
        if not day_jobs:
            logger.info("Day %s: no jobs, skipping", day_str)
            days_skipped_no_jobs += 1
            continue

        day_ce_raw = costs_by_day.get(day_str)
        if not use_fallback and not day_ce_raw:
            logger.info("Day %s: no CE data, skipping (%d jobs ignored)", day_str, len(day_jobs))
            days_skipped_no_ce += 1
            continue

        # Build per-day config
        day_config = build_config(day_str, redis_password, asg_prefix, region, use_fallback, cost_metric, publish_mode)

        # Compute queue_total_seconds for this day's jobs
        queue_total_seconds: dict[str, float] = defaultdict(float)
        for job in day_jobs:
            queue = safe_get(job, "job", "job_info", "job_queue", default="unknown")
            duration = job_billed_seconds(job)
            queue_total_seconds[queue] += duration

        # Map ASG names to queue names, filter to job queues
        queue_costs: dict[str, float] | None = None
        infra_cost = 0.0

        if not use_fallback and day_ce_raw:
            ce_stripped = {}
            for asg_name, cost in day_ce_raw.items():
                queue_name = asg_name_to_queue(asg_name, asg_prefix)
                ce_stripped[queue_name] = ce_stripped.get(queue_name, 0) + cost

            matched, unmatched = apply_queue_map(ce_stripped, set(queue_total_seconds), queue_map)
            infra_cost = sum(unmatched.values())
            queue_costs = matched

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Day %s: CE queue names matched (%d): %s",
                    day_str, len(matched),
                    ", ".join(f"{q} (${c:.4f})" for q, c in sorted(matched.items(), key=lambda x: -x[1])),
                )
                logger.debug(
                    "Day %s: CE queue names UNMATCHED (%d, $%.4f total): %s",
                    day_str, len(unmatched), infra_cost,
                    ", ".join(f"{q} (${c:.4f})" for q, c in sorted(unmatched.items(), key=lambda x: -x[1])),
                )
                job_queues_no_ce = {q for q in queue_total_seconds if q not in matched}
                if job_queues_no_ce:
                    logger.debug(
                        "Day %s: Job queues with NO CE match (%d): %s",
                        day_str, len(job_queues_no_ce),
                        ", ".join(sorted(job_queues_no_ce)),
                    )
            elif infra_cost > 0:
                logger.info(
                    "Day %s: excluded $%.4f in non-queue costs (%d unmatched CE names)",
                    day_str, infra_cost, len(unmatched),
                )

        # Calculate per-job costs
        day_job_costs = []
        for job in day_jobs:
            cost = calculate_job_cost(
                job,
                ec2_costs={},                    # Not used: CE tag-grouped path uses queue_costs
                ebs_costs_by_queue={},           # Not used: EBS bundled into queue_costs from CE
                queue_total_job_seconds=dict(queue_total_seconds),
                config=day_config,
                queue_costs=queue_costs,
            )
            day_job_costs.append(cost)

        day_total_job_cost = sum(c["total"] for c in day_job_costs)
        grand_total_job_cost += day_total_job_cost

        # Build job cost documents
        day_documents = []
        for job, cost in zip(day_jobs, day_job_costs):
            doc = build_job_cost_document(
                job, cost,
                day_config.window_start, day_config.window_end,
                day_config.run_id,
                cost_metric=day_config.cost_metric,
            )
            day_documents.append(doc)

        # Calculate queue overhead
        queue_totals = compute_queue_totals(day_jobs, day_job_costs)
        day_total_overhead = 0.0

        if queue_costs:
            for queue, q_cost in queue_costs.items():
                totals = queue_totals.get(queue, {})
                job_cost_sum = totals.get("total_ec2", 0) + totals.get("total_ebs", 0)
                overhead_cost = max(0.0, q_cost - job_cost_sum)
                day_total_overhead += overhead_cost

                overhead = calculate_queue_overhead(
                    queue=queue,
                    total_instance_cost=q_cost,
                    total_job_ec2_cost=job_cost_sum,
                    total_ebs_cost=0.0,             # EBS bundled into CE tag cost; cannot split
                    total_job_ebs_cost=0.0,         # Same: EBS not separated from CE total
                    total_instance_hours=24,        # TODO: derive from CE hourly data or instance uptime
                    total_job_hours=totals.get("total_seconds", 0) / 3600,
                    instance_count=0,               # TODO: count unique instances per queue from job data
                    job_count=int(totals.get("job_count", 0)),
                )
                doc = build_queue_overhead_document(
                    overhead,
                    day_config.window_start, day_config.window_end,
                    day_config.run_id,
                    cost_metric=day_config.cost_metric,
                )
                day_documents.append(doc)

        grand_total_overhead += day_total_overhead
        grand_total_infra += infra_cost

        all_documents.extend(day_documents)
        days_processed += 1

        ce_day_total = (sum(queue_costs.values()) if queue_costs else 0.0) + infra_cost
        logger.info(
            "Day %s: %d jobs, $%.4f job cost, $%.4f overhead, $%.4f CE total, %d docs",
            day_str, len(day_jobs), day_total_job_cost, day_total_overhead,
            ce_day_total, len(day_documents),
        )

    # Step 6: Publish all documents
    if not all_documents:
        logger.info("No documents generated for date range. Nothing to publish.")
        click.echo("No documents generated for the date range.")
        return

    if output_json:
        write_documents_json(all_documents, output_json)
    if dry_run:
        logger.info("DRY RUN -- skipping publish of %d documents", len(all_documents))
    else:
        published = publish(all_documents, temp_config)
        logger.info("Published %d documents (mode=%s)", published, temp_config.publish_mode)

    # Step 7: Summary
    total_days = (dt_end - dt_start).days + 1
    ce_grand_total = grand_total_job_cost + grand_total_overhead + grand_total_infra

    click.echo(f"\nDate Range Backfill Summary: {start_date} to {end_date}")
    click.echo(f"{'=' * 55}")
    click.echo(f"Cost metric:         {cost_metric}")
    click.echo(f"Total days in range: {total_days}")
    click.echo(f"Days processed:      {days_processed}")
    click.echo(f"Days skipped (no jobs): {days_skipped_no_jobs}")
    if not use_fallback:
        click.echo(f"Days skipped (no CE):   {days_skipped_no_ce}")
    click.echo(f"Total jobs:          {len(all_jobs)}")
    if include_retries:
        retry_count = sum(1 for j in all_jobs if j.get("is_retry_attempt"))
        if retry_count:
            click.echo(f"  Retry attempts:    {retry_count}")
    if not use_fallback:
        click.echo(f"CE total (all):      ${ce_grand_total:.4f}")
        click.echo(f"  Job queues:        ${grand_total_job_cost + grand_total_overhead:.4f}")
        click.echo(f"  Infra (excl):      ${grand_total_infra:.4f}  (pcm-*, etc.)")
    click.echo(f"Total job cost:      ${grand_total_job_cost:.4f}")
    click.echo(f"Total overhead:      ${grand_total_overhead:.4f}")
    click.echo(f"Documents:           {len(all_documents)}")
    if output_json:
        click.echo(f"JSON output:         {output_json}")
    if dry_run:
        click.echo("Mode:                DRY RUN (not published)")
    else:
        target = "metrics OpenSearch (direct bulk)" if temp_config.publish_mode == "direct" else "Redis"
        click.echo(f"Mode:                PUBLISHED to {target}")


def fetch_usage_hourly_and_daily(
    ce_client,
    ce_start: datetime,
    ce_end_exclusive: datetime,
    asg_prefix: str,
    cost_metric: str,
) -> tuple[dict, dict]:
    """Fetch HOURLY usage where CE retains it, and DAILY usage for the full span."""
    daily = fetch_queue_usage(
        ce_client, ce_start.strftime("%Y-%m-%d"), ce_end_exclusive.strftime("%Y-%m-%d"),
        "DAILY", asg_prefix, cost_metric,
    )
    logger.info("Fetched DAILY CE cost + running hours for %d days", len(daily))

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    hourly_start = max(ce_start, today - timedelta(days=CE_HOURLY_RETENTION_DAYS))
    hourly: dict = {}
    # The retention boundary is not exact; step forward a day if CE rejects it
    for _ in range(2):
        if hourly_start >= ce_end_exclusive:
            break
        try:
            hourly = fetch_queue_usage(
                ce_client,
                hourly_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ce_end_exclusive.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "HOURLY", asg_prefix, cost_metric,
            )
            break
        except Exception as e:
            logger.warning("HOURLY CE query from %s failed: %s", hourly_start.date(), e)
            hourly_start += timedelta(days=1)
    if hourly:
        logger.info(
            "Fetched HOURLY CE cost + running hours for %d hours (%s onward); "
            "earlier hours use DAILY rates",
            len(hourly), hourly_start.strftime("%Y-%m-%d"),
        )
    else:
        logger.warning("No HOURLY CE data available; all hours use DAILY rates")
    return hourly, daily


def run_date_range_hourly(
    start_date: str,
    end_date: str,
    redis_password: str | None,
    asg_prefix: str,
    region: str,
    include_retries: bool,
    dry_run: bool,
    skip_job_types: tuple[str, ...] = (),
    cost_metric: str = "AmortizedCost",
    queue_map: dict[str, str] | None = None,
    publish_mode: str = "redis",
    output_json: str | None = None,
    idle_to_jobs: bool = True,
) -> None:
    """Attribute cost per queue-hour using CE running instance-seconds.

    Job documents go to the day of the job's time_end (same as the daily
    path), but each job is charged for the hours it actually ran. With
    idle_to_jobs, each hour's idle cost is split among the jobs that ran in
    that hour; queue overhead documents keep only hours with no jobs.
    Otherwise all idle cost is queue overhead.
    """
    logger.info("Starting hourly attribution: %s to %s", start_date, end_date)

    try:
        dt_start = datetime.strptime(start_date, "%Y-%m-%d")
        dt_end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        raise click.ClickException(f"Invalid date format: {e}")
    if dt_start > dt_end:
        raise click.ClickException(f"Start date {start_date} is after end date {end_date}")

    report_days = {
        (dt_start + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((dt_end - dt_start).days + 1)
    }
    pad = timedelta(days=HOURLY_JOB_PAD_DAYS)
    range_end_exclusive = dt_end + timedelta(days=1)

    temp_config = build_config(start_date, redis_password, asg_prefix, region, False, cost_metric, publish_mode)
    if not dry_run:
        preflight_publish(temp_config)

    # CE: from pad days before the range (jobs that started earlier) to range end
    ce_client = create_ce_client(temp_config)
    hourly_usage, daily_usage = fetch_usage_hourly_and_daily(
        ce_client, dt_start - pad, range_end_exclusive, asg_prefix, temp_config.cost_metric,
    )

    # Jobs: ending pad days before the range through pad days after it. The
    # extra jobs get no documents, but they make each hour's occupancy
    # complete: later jobs occupy in-range hours, earlier jobs occupy the
    # pre-range hours that range jobs which started early also ran in.
    fetch_start = dt_start - pad
    fetch_end = dt_end + pad
    range_config = AttributionConfig(
        window_start=f"{fetch_start.strftime('%Y-%m-%d')}T00:00:00Z",
        window_end=f"{fetch_end.strftime('%Y-%m-%d')}T23:59:59Z",
        run_id=f"hourly-{start_date}-to-{end_date}",
        redis_password=redis_password,
        asg_prefix=asg_prefix,
        aws_region=region,
        cost_metric=cost_metric,
        publish_mode=publish_mode,
    )
    all_jobs = fetch_all_jobs(range_config)
    logger.info(
        "Fetched %d jobs ending %s to %s",
        len(all_jobs), fetch_start.strftime("%Y-%m-%d"), fetch_end.strftime("%Y-%m-%d"),
    )
    if include_retries:
        extra = fetch_metrics_extra_attempts(range_config, all_jobs)
        if extra:
            all_jobs.extend(extra)
            logger.info("Added %d retry attempts, total jobs now: %d", len(extra), len(all_jobs))
    all_jobs = filter_skipped_job_types(all_jobs, skip_job_types)

    allocation = allocate_hourly(
        all_jobs, hourly_usage, daily_usage, report_days, asg_prefix, queue_map,
        idle_to_jobs=idle_to_jobs,
        occupancy_complete_from=fetch_start.replace(tzinfo=timezone.utc),
    )
    if allocation.over_occupied_hours:
        logger.warning(
            "%d queue-hours had more job-seconds than CE running instance-seconds "
            "(max ratio %.2fx); those hours are charged at cost / job-seconds: %s",
            allocation.over_occupied_hours, allocation.max_occupancy_ratio,
            ", ".join(f"{q} ({n})" for q, n in sorted(allocation.over_occupied_queues.items(), key=lambda x: -x[1])),
        )
    if allocation.unpriced_job_seconds:
        logger.warning(
            "%.1f job-hours in the range fell in queue-hours with no CE cost and were charged $0",
            allocation.unpriced_job_seconds / 3600,
        )

    # Build documents per report day
    jobs_by_day: dict[str, list[tuple[dict, dict]]] = defaultdict(list)
    for job, alloc in zip(all_jobs, allocation.job_costs):
        time_end = safe_get(job, "job", "job_info", "time_end")
        if time_end and str(time_end)[:10] in report_days:
            jobs_by_day[str(time_end)[:10]].append((job, alloc))

    all_documents: list[dict] = []
    grand = defaultdict(float)
    report_jobs = 0

    for day_str in sorted(report_days):
        day_config = build_config(day_str, redis_password, asg_prefix, region, False, cost_metric, publish_mode)
        day_job_cost = 0.0
        for job, alloc in jobs_by_day.get(day_str, []):
            cost = build_cost_breakdown(
                job, alloc["ec2_compute"], 0.0, alloc["ec2_hourly_rate"], alloc["pricing_source"],
                ec2_idle_share=alloc["ec2_idle_share"],
            )
            day_job_cost += cost["total"]
            all_documents.append(build_job_cost_document(
                job, cost, day_config.window_start, day_config.window_end,
                day_config.run_id, cost_metric=day_config.cost_metric,
            ))
        report_jobs += len(jobs_by_day.get(day_str, []))

        day_queue_cost = day_overhead = day_in_day_job_cost = 0.0
        for (queue, qd_day), qd in sorted(allocation.queue_days.items()):
            if qd_day != day_str:
                continue
            overhead = calculate_queue_overhead(
                queue=queue,
                total_instance_cost=qd.cost,
                total_job_ec2_cost=qd.job_cost,
                total_ebs_cost=0.0,             # EBS bundled into CE tag cost
                total_job_ebs_cost=0.0,
                total_instance_hours=qd.instance_seconds / 3600,
                total_job_hours=qd.job_seconds / 3600,
                instance_count=len(qd.instances),
                job_count=len(qd.job_ids),
            )
            overhead["overhead_details"]["idle_redistributed_to_jobs"] = round(qd.idle_to_jobs, 6)
            all_documents.append(build_queue_overhead_document(
                overhead, day_config.window_start, day_config.window_end,
                day_config.run_id, cost_metric=day_config.cost_metric,
            ))
            day_queue_cost += qd.cost
            grand["idle_to_jobs"] += qd.idle_to_jobs
            day_in_day_job_cost += qd.job_cost
            day_overhead += overhead["cost"]["total"]

        day_infra = allocation.infra_by_day.get(day_str, 0.0)
        grand["job_cost"] += day_job_cost
        grand["queue_cost"] += day_queue_cost
        grand["in_range_job_cost"] += day_in_day_job_cost
        grand["overhead"] += day_overhead
        grand["infra"] += day_infra
        logger.info(
            "Day %s: %d jobs, $%.4f job cost, $%.4f idle overhead, $%.4f queue CE, $%.4f infra (excl)",
            day_str, len(jobs_by_day.get(day_str, [])), day_job_cost, day_overhead,
            day_queue_cost, day_infra,
        )

    if not all_documents:
        click.echo("No documents generated for the date range.")
        return

    if output_json:
        write_documents_json(all_documents, output_json)
    if dry_run:
        logger.info("DRY RUN -- skipping publish of %d documents", len(all_documents))
    else:
        published = publish(all_documents, temp_config)
        logger.info("Published %d documents (mode=%s)", published, temp_config.publish_mode)

    retry_count = sum(
        1 for day_jobs in jobs_by_day.values() for job, _ in day_jobs if job.get("is_retry_attempt")
    )
    reconcile_diff = grand["queue_cost"] - grand["in_range_job_cost"] - grand["overhead"]

    click.echo(f"\nHourly Attribution Summary: {start_date} to {end_date}")
    click.echo(f"{'=' * 55}")
    click.echo(f"Cost metric:         {cost_metric}")
    click.echo(f"Hours priced hourly: {len(hourly_usage)}")
    click.echo(f"Total jobs:          {report_jobs}")
    if include_retries:
        click.echo(f"  Retry attempts:    {retry_count}")
    click.echo(f"CE total (all):      ${grand['queue_cost'] + grand['infra']:.4f}")
    click.echo(f"  Job queues:        ${grand['queue_cost']:.4f}")
    click.echo(f"    Job hours:       ${grand['in_range_job_cost']:.4f}")
    if idle_to_jobs:
        click.echo(f"      (idle split to jobs: ${grand['idle_to_jobs']:.4f})")
    click.echo(f"    Idle overhead:   ${grand['overhead']:.4f}" + ("  (hours with no jobs)" if idle_to_jobs else ""))
    click.echo(f"    Unreconciled:    ${reconcile_diff:.4f}")
    click.echo(f"  Infra (excl):      ${grand['infra']:.4f}  (pcm-*, etc.)")
    click.echo(f"Total job cost:      ${grand['job_cost']:.4f}  (incl. S3 and hours outside the range)")
    click.echo(f"Documents:           {len(all_documents)}")
    if output_json:
        click.echo(f"JSON output:         {output_json}")
    if dry_run:
        click.echo("Mode:                DRY RUN (not published)")
    else:
        target = "metrics OpenSearch (direct bulk)" if temp_config.publish_mode == "direct" else "Redis"
        click.echo(f"Mode:                PUBLISHED to {target}")


@click.command()
@click.option(
    "--date",
    default=None,
    help="Date to process (YYYY-MM-DD). Defaults to 3 days ago (T-3). Mutually exclusive with --date-range.",
)
@click.option(
    "--date-range",
    nargs=2,
    type=str,
    default=None,
    help="Date range to backfill (START END, YYYY-MM-DD). Single CE call, processes each day. Mutually exclusive with --date.",
)
@click.option(
    "--redis-password",
    envvar="REDIS_PASSWORD",
    default=None,
    help="Redis password (or set REDIS_PASSWORD env var).",
)
@click.option(
    "--asg-prefix",
    default="",
    help="ASG name prefix to strip when mapping to queue names.",
)
@click.option(
    "--queue-map-file",
    type=click.Path(exists=True),
    default=None,
    help="JSON file mapping HySDS queue names to CE/ASG tag names (after prefix strip). "
         'E.g., {"smap-pge-radiometer": "smap-pge-radiometer-workflow"}',
)
@click.option(
    "--region",
    default="us-west-2",
    help="AWS region for Cost Explorer.",
)
@click.option(
    "--use-fallback",
    is_flag=True,
    default=False,
    help="Force fallback pricing (skip Cost Explorer).",
)
@click.option(
    "--cost-metric",
    type=click.Choice(["UnblendedCost", "AmortizedCost", "BlendedCost"], case_sensitive=True),
    default="AmortizedCost",
    help="AWS Cost Explorer metric (default: AmortizedCost). Use AmortizedCost for reserved instances.",
)
@click.option(
    "--estimate-recent",
    is_flag=True,
    default=False,
    help="Hourly estimator mode: estimate costs for recent terminal jobs without cost docs.",
)
@click.option(
    "--include-retries",
    is_flag=True,
    default=False,
    help="Include overwritten retry attempts from metrics in cost attribution.",
)
@click.option(
    "--skip-job-types",
    multiple=True,
    default=(),
    help="Job type prefixes to exclude from cost attribution (repeatable). "
         "E.g., --skip-job-types job-workflow skips all job-workflow:* jobs.",
)
@click.option(
    "--direct",
    "publish_mode_flag",
    flag_value="direct",
    default=None,
    help="Bulk-index documents directly to the metrics OpenSearch cluster, bypassing Redis/Logstash.",
)
@click.option(
    "--redis",
    "publish_mode_flag",
    flag_value="redis",
    help="Publish documents via Redis for Logstash ingestion (default).",
)
@click.option(
    "--hourly",
    is_flag=True,
    default=False,
    help="Allocate per queue-hour from CE hourly cost / running instance-seconds; "
         "idle time becomes queue overhead. Hours older than 14 days use daily rates.",
)
@click.option(
    "--hourly-idle",
    type=click.Choice(["jobs", "overhead"]),
    default="jobs",
    show_default=True,
    help="With --hourly: 'jobs' splits each hour's idle cost among the jobs that ran "
         "that hour (per second); 'overhead' leaves it as queue overhead.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Calculate costs but don't publish to Redis.",
)
@click.option(
    "--output-json",
    type=click.Path(dir_okay=False, writable=True),
    default=None,
    help="Write all documents to FILE as JSON Lines instead of publishing (implies --dry-run).",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    default=False,
    help="Enable debug logging.",
)
def main(
    date: str | None,
    date_range: tuple[str, str] | None,
    redis_password: str | None,
    asg_prefix: str,
    queue_map_file: str | None,
    region: str,
    use_fallback: bool,
    cost_metric: str,
    estimate_recent: bool,
    include_retries: bool,
    skip_job_types: tuple[str, ...],
    publish_mode_flag: str | None,
    hourly: bool,
    hourly_idle: str,
    dry_run: bool,
    output_json: str | None,
    verbose: bool,
) -> None:
    """MAAP HySDS Job Cost Attribution Batch Process.

    Fetches AWS costs, attributes them to individual jobs, and publishes
    results to Redis for Logstash → OpenSearch ingestion.
    """
    setup_logging(verbose)

    publish_mode = publish_mode_flag or "redis"

    # JSON output replaces publishing
    if output_json:
        dry_run = True

    # Load queue map if provided
    queue_map = load_queue_map(queue_map_file) if queue_map_file else None

    # Mutual exclusivity check
    if date and date_range:
        raise click.ClickException("--date and --date-range are mutually exclusive.")

    if estimate_recent:
        run_hourly_estimator(
            region=region,
            asg_prefix=asg_prefix,
            dry_run=dry_run,
            verbose=verbose,
            skip_job_types=skip_job_types,
            cost_metric=cost_metric,
            publish_mode=publish_mode,
            output_json=output_json,
        )
        return

    if hourly:
        if use_fallback:
            raise click.ClickException("--hourly requires Cost Explorer; it cannot be combined with --use-fallback.")
        if date is None and not date_range:
            date = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
        start, end = date_range if date_range else (date, date)
        run_date_range_hourly(
            start_date=start,
            end_date=end,
            redis_password=redis_password,
            asg_prefix=asg_prefix,
            region=region,
            include_retries=include_retries,
            dry_run=dry_run,
            skip_job_types=skip_job_types,
            cost_metric=cost_metric,
            queue_map=queue_map,
            publish_mode=publish_mode,
            output_json=output_json,
            idle_to_jobs=hourly_idle == "jobs",
        )
        return

    if date_range:
        run_date_range(
            start_date=date_range[0],
            end_date=date_range[1],
            redis_password=redis_password,
            asg_prefix=asg_prefix,
            region=region,
            use_fallback=use_fallback,
            include_retries=include_retries,
            dry_run=dry_run,
            skip_job_types=skip_job_types,
            cost_metric=cost_metric,
            queue_map=queue_map,
            publish_mode=publish_mode,
            output_json=output_json,
        )
        return

    # Default to 3 days ago (T-3)
    if date is None:
        target = datetime.now(timezone.utc) - timedelta(days=3)
        date = target.strftime("%Y-%m-%d")

    logger.info("Starting cost attribution for %s", date)
    config = build_config(date, redis_password, asg_prefix, region, use_fallback, cost_metric, publish_mode)
    logger.info("Run ID: %s", config.run_id)
    logger.info("Window: %s → %s", config.window_start, config.window_end)

    # Step 1: Check publisher connectivity (unless dry run)
    if not dry_run:
        preflight_publish(config)

    # Step 2: Fetch jobs from Mozart
    jobs = fetch_all_jobs(config)
    if not jobs:
        logger.info("No jobs found in window. Nothing to do.")
        return

    # Step 2b: Optionally supplement with retry attempts from metrics
    if include_retries:
        extra = fetch_metrics_extra_attempts(config, jobs)
        if extra:
            jobs.extend(extra)
            logger.info("Added %d retry attempts, total jobs now: %d", len(extra), len(jobs))

    # Step 2c: Filter out skipped job types
    jobs = filter_skipped_job_types(jobs, skip_job_types)

    # Step 3: Pre-compute total job seconds per queue
    queue_total_seconds: dict[str, float] = defaultdict(float)
    for job in jobs:
        queue = safe_get(job, "job", "job_info", "job_queue", default="unknown")
        duration = job_billed_seconds(job)
        queue_total_seconds[queue] += duration

    # Step 4: Fetch costs from Cost Explorer (single tag-grouped call)
    queue_costs: dict[str, float] | None = None
    infra_cost = 0.0

    if not use_fallback:
        ce_client = create_ce_client(config)
        # CE end date is exclusive, so add 1 day
        ce_start = date
        ce_end = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        raw_costs = fetch_queue_costs(ce_client, ce_start, ce_end, asg_prefix, cost_metric=config.cost_metric)
        logger.info("Got CE tag-grouped costs for %d ASGs", len(raw_costs))

        # Map raw ASG names to queue names, keep only actual job queues
        ce_stripped = {}
        for asg_name, cost in raw_costs.items():
            queue_name = asg_name_to_queue(asg_name, asg_prefix)
            ce_stripped[queue_name] = ce_stripped.get(queue_name, 0) + cost

        # Match CE names to job queues (using queue map if provided)
        matched, unmatched = apply_queue_map(ce_stripped, set(queue_total_seconds), queue_map)
        infra_cost = sum(unmatched.values())
        queue_costs = matched

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("CE queue names MATCHED (%d):", len(matched))
            for q, c in sorted(matched.items(), key=lambda x: -x[1]):
                logger.debug("  %-50s $%.4f", q, c)
            logger.debug("CE queue names UNMATCHED (%d, $%.4f total):", len(unmatched), infra_cost)
            for q, c in sorted(unmatched.items(), key=lambda x: -x[1]):
                logger.debug("  %-50s $%.4f", q, c)
            job_queues_no_ce = {q for q in queue_total_seconds if q not in matched}
            if job_queues_no_ce:
                logger.debug("Job queues with NO CE match (%d):", len(job_queues_no_ce))
                for q in sorted(job_queues_no_ce):
                    logger.debug("  %s  (%.1f job-seconds)", q, queue_total_seconds[q])
        elif infra_cost > 0:
            logger.info("Excluded $%.4f in non-queue costs (%d unmatched CE names)", infra_cost, len(unmatched))

    # Step 5: Calculate per-job costs
    job_costs = []
    for job in jobs:
        cost = calculate_job_cost(
            job,
            ec2_costs={},                    # Not used: CE tag-grouped path uses queue_costs
            ebs_costs_by_queue={},           # Not used: EBS bundled into queue_costs from CE
            queue_total_job_seconds=dict(queue_total_seconds),
            config=config,
            queue_costs=queue_costs,
        )
        job_costs.append(cost)

    total_job_cost = sum(c["total"] for c in job_costs)
    logger.info(
        "Calculated costs for %d jobs, total: $%.4f",
        len(job_costs), total_job_cost,
    )

    # Step 6: Build job cost documents
    documents = []
    for job, cost in zip(jobs, job_costs):
        doc = build_job_cost_document(
            job, cost,
            config.window_start, config.window_end,
            config.run_id,
            cost_metric=config.cost_metric,
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Job %s: cost=$%.6f (EC2 $%.4f, EBS $%.4f) source=%s",
                doc.get("job_id"),
                doc.get("cost", {}).get("total", 0),
                doc.get("cost", {}).get("ec2", 0),
                doc.get("cost", {}).get("ebs", 0),
                doc.get("cost_inputs", {}).get("pricing_source"),
            )
        documents.append(doc)

    # Step 7: Calculate queue overhead (CE total - attributed job costs)
    queue_totals = compute_queue_totals(jobs, job_costs)
    total_overhead = 0.0

    if queue_costs:
        for queue, q_cost in queue_costs.items():
            totals = queue_totals.get(queue, {})
            job_cost_sum = totals.get("total_ec2", 0) + totals.get("total_ebs", 0)
            # Overhead = CE queue cost - job infra costs (S3 is separate)
            overhead_cost = max(0.0, q_cost - job_cost_sum)
            total_overhead += overhead_cost

            overhead = calculate_queue_overhead(
                queue=queue,
                total_instance_cost=q_cost,
                total_job_ec2_cost=job_cost_sum,
                total_ebs_cost=0.0,             # EBS bundled into CE tag cost; cannot split
                total_job_ebs_cost=0.0,         # Same: EBS not separated from CE total
                total_instance_hours=24,        # TODO: derive from CE hourly data or instance uptime
                total_job_hours=totals.get("total_seconds", 0) / 3600,
                instance_count=0,               # TODO: count unique instances per queue from job data
                job_count=int(totals.get("job_count", 0)),
            )
            doc = build_queue_overhead_document(
                overhead,
                config.window_start, config.window_end,
                config.run_id,
                cost_metric=config.cost_metric,
            )
            documents.append(doc)

    ce_total_queues = sum(queue_costs.values()) if queue_costs else 0.0
    ce_total = ce_total_queues + (infra_cost if not use_fallback else 0.0)

    logger.info(
        "Built %d documents (%d job costs + %d queue overheads)",
        len(documents), len(job_costs), len(documents) - len(job_costs),
    )

    # Step 10: Publish
    if output_json:
        write_documents_json(documents, output_json)
    if dry_run:
        logger.info("DRY RUN — skipping publish")
        for doc in documents[:3]:
            logger.debug("Sample document: %s", doc.get("doc_type"))
    else:
        published = publish(documents, config)
        logger.info("Published %d documents (mode=%s)", published, config.publish_mode)

    # Summary
    click.echo(f"\nCost Attribution Summary for {date}")
    click.echo(f"{'=' * 45}")
    click.echo(f"Cost metric:       {config.cost_metric}")
    click.echo(f"Jobs processed:    {len(job_costs)}")
    if include_retries:
        retry_count = sum(1 for j in jobs if j.get("is_retry_attempt"))
        if retry_count:
            click.echo(f"  Retry attempts:  {retry_count}")
    click.echo(f"Total job cost:    ${total_job_cost:.4f}")
    click.echo(f"Total overhead:    ${total_overhead:.4f}")
    click.echo(f"CE total (tag):    ${ce_total:.4f}")
    if not use_fallback and infra_cost > 0:
        click.echo(f"  Worker queues:   ${ce_total_queues:.4f}")
        click.echo(f"  Infrastructure:  ${infra_cost:.4f}  (pcm-*, etc.)")
    click.echo(f"Queues:            {len(queue_totals)}")
    click.echo(f"Documents:         {len(documents)}")
    click.echo(f"Run ID:            {config.run_id}")
    if output_json:
        click.echo(f"JSON output:       {output_json}")
    if dry_run:
        click.echo("Mode:              DRY RUN (not published)")
    else:
        target = "metrics OpenSearch (direct bulk)" if config.publish_mode == "direct" else "Redis"
        click.echo(f"Mode:              PUBLISHED to {target}")


if __name__ == "__main__":
    main()
