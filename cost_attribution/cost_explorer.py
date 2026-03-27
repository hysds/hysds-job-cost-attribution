"""AWS Cost Explorer client for fetching EC2 and EBS costs."""

import json
import logging
from typing import Any

import boto3

from .config import AttributionConfig

logger = logging.getLogger(__name__)


def create_ce_client(config: AttributionConfig):
    """Create a Cost Explorer client."""
    return boto3.client("ce", region_name=config.aws_region)


def fetch_queue_costs_by_day(
    ce_client,
    start_date: str,
    end_date: str,
    asg_prefix: str = "",
    cost_metric: str = "AmortizedCost",
) -> dict[str, dict[str, float]]:
    """Fetch daily costs grouped by Name tag, keyed by date.

    Makes ONE Cost Explorer API call for the full date range and returns
    per-day cost breakdowns. Useful for backfilling multiple days efficiently.

    Args:
        ce_client: Boto3 Cost Explorer client
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD (exclusive in CE API)
        asg_prefix: Only include names starting with this prefix (client-side filter)

    Returns:
        Dict mapping date (YYYY-MM-DD) -> {raw ASG/tag name -> cost in USD}
    """
    costs_by_day: dict[str, dict[str, float]] = {}

    try:
        response = ce_client.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="DAILY",
            Metrics=[cost_metric],
            GroupBy=[{"Type": "TAG", "Key": "Name"}],
        )

        for result in response.get("ResultsByTime", []):
            day = result["TimePeriod"]["Start"]
            day_costs: dict[str, float] = {}

            for group in result.get("Groups", []):
                tag_key = group["Keys"][0]
                if "$" in tag_key:
                    name = tag_key.split("$", 1)[1]
                else:
                    name = tag_key
                if not name:
                    continue
                if asg_prefix and not name.startswith(asg_prefix):
                    continue
                amount = float(group["Metrics"][cost_metric]["Amount"])
                day_costs[name] = day_costs.get(name, 0) + amount

            costs_by_day[day] = day_costs

    except Exception as e:
        logger.warning("Cost Explorer tag-grouped query failed: %s", e)

    return costs_by_day


def fetch_queue_costs(
    ce_client,
    start_date: str,
    end_date: str,
    asg_prefix: str = "",
    cost_metric: str = "AmortizedCost",
) -> dict[str, float]:
    """Fetch daily costs grouped by Name tag from Cost Explorer.

    A single CE call with TAG:Name grouping captures all costs tagged with
    that Name — EC2 compute, EBS, Inspector, licensing — matching what the
    AWS CE console shows when filtered by tag.

    Args:
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD (exclusive in CE API)
        asg_prefix: Only include names starting with this prefix (client-side filter)

    Returns:
        Dict mapping raw ASG/tag name -> total daily cost in USD
    """
    costs: dict[str, float] = {}

    try:
        response = ce_client.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="DAILY",
            Metrics=[cost_metric],
            GroupBy=[{"Type": "TAG", "Key": "Name"}],
        )

        for result in response.get("ResultsByTime", []):
            for group in result.get("Groups", []):
                tag_key = group["Keys"][0]
                # Tag key format: "Name$value"
                if "$" in tag_key:
                    name = tag_key.split("$", 1)[1]
                else:
                    name = tag_key
                if not name:
                    continue
                # Client-side filter to MAAP ASGs
                if asg_prefix and not name.startswith(asg_prefix):
                    continue
                amount = float(group["Metrics"][cost_metric]["Amount"])
                costs[name] = costs.get(name, 0) + amount

    except Exception as e:
        logger.warning("Cost Explorer tag-grouped query failed: %s", e)

    return costs



def fetch_spot_hourly_rates(
    instance_types: list[str],
    availability_zones: list[str],
    region: str = "us-west-2",
    lookback_hours: int = 24,
) -> dict[str, float]:
    """Fetch recent spot hourly rates by averaging spot price history.

    Args:
        instance_types: EC2 instance types to look up
        availability_zones: AZs to average across
        region: AWS region
        lookback_hours: Hours of history to average

    Returns:
        Dict mapping instance_type -> average hourly rate in USD
    """
    from datetime import datetime, timedelta, timezone

    ec2_client = boto3.client("ec2", region_name=region)
    start_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    # Collect all price points per instance type
    prices: dict[str, list[float]] = {t: [] for t in instance_types}
    next_token = ""

    while True:
        kwargs: dict[str, Any] = {
            "InstanceTypes": instance_types,
            "ProductDescriptions": ["Linux/UNIX"],
            "StartTime": start_time,
            "MaxResults": 1000,
        }
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            response = ec2_client.describe_spot_price_history(**kwargs)
        except Exception as e:
            logger.warning("Spot price history query failed: %s", e)
            break

        for record in response.get("SpotPriceHistory", []):
            itype = record["InstanceType"]
            price = float(record["SpotPrice"])
            if itype in prices:
                prices[itype].append(price)

        next_token = response.get("NextToken", "")
        if not next_token:
            break

    # Average prices per instance type
    rates: dict[str, float] = {}
    for itype, price_list in prices.items():
        if price_list:
            rates[itype] = sum(price_list) / len(price_list)

    logger.info(
        "Fetched spot pricing for %d/%d instance types (%d total price points)",
        len(rates), len(instance_types),
        sum(len(v) for v in prices.values()),
    )
    return rates


REGION_TO_LOCATION = {
    "us-east-1": "US East (N. Virginia)",
    "us-east-2": "US East (Ohio)",
    "us-west-1": "US West (N. California)",
    "us-west-2": "US West (Oregon)",
}


def fetch_ondemand_hourly_rates(
    instance_types: list[str],
    region: str = "us-west-2",
) -> dict[str, float]:
    """Fetch on-demand hourly rates from the AWS Pricing API.

    Args:
        instance_types: EC2 instance types (e.g., ["m5d.large", "m6a.large"])
        region: AWS region code (e.g., "us-west-2")

    Returns:
        Dict mapping instance_type -> hourly rate in USD
    """
    location = REGION_TO_LOCATION.get(region)
    if not location:
        logger.warning("No location mapping for region %s, cannot fetch pricing", region)
        return {}

    # Pricing API is only available in us-east-1
    pricing_client = boto3.client("pricing", region_name="us-east-1")
    rates: dict[str, float] = {}

    for instance_type in instance_types:
        try:
            response = pricing_client.get_products(
                ServiceCode="AmazonEC2",
                Filters=[
                    {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                    {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                    {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                    {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                    {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
                    {"Type": "TERM_MATCH", "Field": "capacityStatus", "Value": "Used"},
                ],
                MaxResults=1,
            )

            price_list = response.get("PriceList", [])
            if not price_list:
                logger.warning("No pricing data found for %s in %s", instance_type, region)
                continue

            product = json.loads(price_list[0])
            on_demand = product.get("terms", {}).get("OnDemand", {})
            for offer in on_demand.values():
                for dimension in offer.get("priceDimensions", {}).values():
                    usd = float(dimension["pricePerUnit"]["USD"])
                    if usd > 0:
                        rates[instance_type] = usd
                        break
                if instance_type in rates:
                    break

            if instance_type not in rates:
                logger.warning("Could not parse price for %s", instance_type)

        except Exception as e:
            logger.warning("Pricing API query failed for %s: %s", instance_type, e)

    logger.info(
        "Fetched pricing for %d/%d instance types via AWS Pricing API",
        len(rates), len(instance_types),
    )
    return rates


def get_instance_daily_cost(
    instance_id: str,
    instance_type: str,
    ce_costs: dict[str, float],
    pricing_rates: dict[str, float],
    use_fallback: bool = False,
    spot_types: set[str] | None = None,
    instance_counts: dict[str, int] | None = None,
) -> tuple[float, str]:
    """Get the daily cost for an instance, with fallback.

    Fallback chain: CE type costs → pricing_rates (spot/on-demand) → $0.00 warning

    ce_costs maps instance_type -> total daily cost for all instances of that type.
    instance_counts maps instance_type -> number of instances, used to derive
    per-instance cost from the type-level aggregate.

    Returns:
        Tuple of (daily_cost, pricing_source)
        pricing_source is "cost_explorer", "spot_history", or "ondemand_api"
    """
    if not use_fallback and instance_type in ce_costs:
        count = (instance_counts or {}).get(instance_type, 1)
        return ce_costs[instance_type] / count, "cost_explorer"

    # Fallback to spot/on-demand pricing rates
    hourly_rate = pricing_rates.get(instance_type)
    if hourly_rate is not None:
        source = "spot_history" if spot_types and instance_type in spot_types else "ondemand_api"
        return hourly_rate * 24, source

    logger.warning(
        "No cost data for instance %s (type: %s), using zero",
        instance_id, instance_type,
    )
    return 0.0, "ondemand_api"
