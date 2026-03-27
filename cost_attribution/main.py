"""CLI entry point for the cost attribution batch process."""
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import click

from .config import AttributionConfig
from .cost_explorer import (
    create_ce_client,
    fetch_queue_costs,
    fetch_queue_costs_by_day,
    fetch_ondemand_hourly_rates,
    fetch_spot_hourly_rates,
)
from .calculator import (
    calculate_job_cost,
    calculate_queue_overhead,
    compute_queue_totals,
)
from .mozart_client import fetch_all_jobs, fetch_metrics_extra_attempts
from .redis_publisher import publish_documents, check_redis_connection
from .schema import build_job_cost_document, build_queue_overhead_document
from .utils import asg_name_to_queue, filter_skipped_job_types, safe_get

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def build_config(
    date: str,
    redis_password: str | None,
    asg_prefix: str,
    region: str,
    use_fallback: bool,
    cost_metric: str = "AmortizedCost",
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
    )


def run_hourly_estimator(
    region: str,
    asg_prefix: str,
    dry_run: bool,
    verbose: bool,
    skip_job_types: tuple[str, ...] = (),
    cost_metric: str = "AmortizedCost",
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
    )

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
        published = publish_documents(documents, config)
        logger.info("Published %d estimate documents", published)

    click.echo(f"\nHourly Estimator Summary")
    click.echo(f"{'=' * 40}")
    click.echo(f"Cost metric:       {config.cost_metric}")
    click.echo(f"Jobs in last 48h:  {len(jobs)}")
    click.echo(f"Already costed:    {len(existing_ids)}")
    click.echo(f"Newly estimated:   {len(documents)}")
    click.echo(f"Estimated total:   ${total_cost:.4f}")
    click.echo(f"Run ID:            {run_id}")
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

    # Step 1: Check Redis connectivity (unless dry run)
    temp_config = build_config(start_date, redis_password, asg_prefix, region, use_fallback, cost_metric)
    if not dry_run:
        if not check_redis_connection(temp_config):
            raise click.ClickException("Cannot connect to Redis. Check connection settings.")

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
        day_config = build_config(day_str, redis_password, asg_prefix, region, use_fallback, cost_metric)

        # Compute queue_total_seconds for this day's jobs
        queue_total_seconds: dict[str, float] = defaultdict(float)
        for job in day_jobs:
            queue = safe_get(job, "job", "job_info", "job_queue", default="unknown")
            duration = safe_get(job, "job", "job_info", "duration", default=0) or 0
            queue_total_seconds[queue] += duration

        # Map ASG names to queue names, filter to job queues
        queue_costs: dict[str, float] | None = None
        infra_cost = 0.0

        if not use_fallback and day_ce_raw:
            queue_costs = {}
            for asg_name, cost in day_ce_raw.items():
                queue_name = asg_name_to_queue(asg_name, asg_prefix)
                queue_costs[queue_name] = queue_costs.get(queue_name, 0) + cost

            infra_cost = sum(c for q, c in queue_costs.items() if q not in queue_total_seconds)
            queue_costs = {q: c for q, c in queue_costs.items() if q in queue_total_seconds}
            if infra_cost > 0:
                logger.debug(
                    "Day %s: excluded $%.4f in non-queue infrastructure costs",
                    day_str, infra_cost,
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

    if dry_run:
        logger.info("DRY RUN -- skipping Redis publish of %d documents", len(all_documents))
    else:
        published = publish_documents(all_documents, temp_config)
        logger.info("Published %d documents to Redis", published)

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
    click.echo(f"Total job cost:      ${grand_total_job_cost:.4f}")
    click.echo(f"Total overhead:      ${grand_total_overhead:.4f}")
    if not use_fallback and grand_total_infra > 0:
        click.echo(f"Total infra (excl):  ${grand_total_infra:.4f}  (pcm-*, etc.)")
    click.echo(f"Documents:           {len(all_documents)}")
    if dry_run:
        click.echo("Mode:                DRY RUN (not published)")
    else:
        click.echo("Mode:                PUBLISHED to Redis")


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
    "--dry-run",
    is_flag=True,
    default=False,
    help="Calculate costs but don't publish to Redis.",
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
    region: str,
    use_fallback: bool,
    cost_metric: str,
    estimate_recent: bool,
    include_retries: bool,
    skip_job_types: tuple[str, ...],
    dry_run: bool,
    verbose: bool,
) -> None:
    """MAAP HySDS Job Cost Attribution Batch Process.

    Fetches AWS costs, attributes them to individual jobs, and publishes
    results to Redis for Logstash → OpenSearch ingestion.
    """
    setup_logging(verbose)

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
        )
        return

    # Default to 3 days ago (T-3)
    if date is None:
        target = datetime.now(timezone.utc) - timedelta(days=3)
        date = target.strftime("%Y-%m-%d")

    logger.info("Starting cost attribution for %s", date)
    config = build_config(date, redis_password, asg_prefix, region, use_fallback, cost_metric)
    logger.info("Run ID: %s", config.run_id)
    logger.info("Window: %s → %s", config.window_start, config.window_end)

    # Step 1: Check Redis connectivity (unless dry run)
    if not dry_run:
        if not check_redis_connection(config):
            raise click.ClickException("Cannot connect to Redis. Check connection settings.")

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
        duration = safe_get(job, "job", "job_info", "duration", default=0) or 0
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
        queue_costs = {}
        for asg_name, cost in raw_costs.items():
            queue_name = asg_name_to_queue(asg_name, asg_prefix)
            queue_costs[queue_name] = queue_costs.get(queue_name, 0) + cost

        # Filter to queues that have jobs (excludes pcm-mozart, pcm-grq, etc.)
        infra_cost = sum(c for q, c in queue_costs.items() if q not in queue_total_seconds)
        queue_costs = {q: c for q, c in queue_costs.items() if q in queue_total_seconds}
        if infra_cost > 0:
            logger.info("Excluded $%.4f in non-queue infrastructure costs (pcm-*, etc.)", infra_cost)

        for q, c in sorted(queue_costs.items(), key=lambda x: -x[1]):
            logger.debug("  Queue %-40s $%.4f", q, c)

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

    # Step 10: Publish to Redis
    if dry_run:
        logger.info("DRY RUN — skipping Redis publish")
        # Print summary
        for doc in documents[:3]:
            logger.debug("Sample document: %s", doc.get("doc_type"))
    else:
        published = publish_documents(documents, config)
        logger.info("Published %d documents to Redis", published)

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
    if dry_run:
        click.echo("Mode:              DRY RUN (not published)")
    else:
        click.echo("Mode:              PUBLISHED to Redis")


if __name__ == "__main__":
    main()
