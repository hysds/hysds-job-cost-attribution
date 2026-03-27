"""Redis publisher for pushing cost attribution documents to Logstash."""

import logging
from typing import Any

import msgpack
import redis

from .config import AttributionConfig

logger = logging.getLogger(__name__)


def create_redis_client(config: AttributionConfig) -> redis.Redis:
    """Create a Redis client."""
    return redis.Redis(
        host=config.redis_host,
        port=config.redis_port,
        password=config.redis_password,
        decode_responses=False,
    )


def publish_documents(
    documents: list[dict[str, Any]],
    config: AttributionConfig,
) -> int:
    """Push cost attribution documents to Redis for Logstash pickup.

    Documents are msgpack-encoded and pushed to the configured Redis key.
    Uses pipeline for batch efficiency.

    Returns:
        Number of documents successfully pushed.
    """
    if not documents:
        logger.info("No documents to publish")
        return 0

    client = create_redis_client(config)
    total_pushed = 0
    batch_size = config.redis_batch_size

    for i in range(0, len(documents), batch_size):
        batch = documents[i : i + batch_size]

        try:
            pipe = client.pipeline(transaction=False)
            for doc in batch:
                packed = msgpack.packb(doc, use_bin_type=True)
                pipe.rpush(config.redis_key, packed)
            pipe.execute()
            total_pushed += len(batch)
            logger.debug(
                "Pushed batch %d-%d (%d docs)",
                i, i + len(batch), len(batch),
            )
        except Exception as e:
            logger.error(
                "Failed to push batch %d-%d: %s", i, i + len(batch), e
            )
            raise

    logger.info(
        "Published %d documents to Redis key '%s'",
        total_pushed, config.redis_key,
    )
    return total_pushed


def check_redis_connection(config: AttributionConfig) -> bool:
    """Verify Redis connectivity."""
    try:
        client = create_redis_client(config)
        client.ping()
        return True
    except Exception as e:
        logger.error("Redis connection failed: %s", e)
        return False
