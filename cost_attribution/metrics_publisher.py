"""Direct OpenSearch bulk publisher for cost attribution documents.

Alternative to the Redis/Logstash pipeline: writes documents straight to the
metrics OpenSearch cluster using the bulk API.
"""

import logging
from typing import Any, Iterator

from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk

from .config import AttributionConfig

logger = logging.getLogger(__name__)


def create_metrics_client(config: AttributionConfig) -> OpenSearch:
    """Create an OpenSearch client for the metrics cluster."""
    return OpenSearch(
        hosts=[{"host": config.metrics_host, "port": config.metrics_port}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
        timeout=60,
    )


def check_metrics_connection(config: AttributionConfig) -> bool:
    """Verify metrics OpenSearch connectivity."""
    try:
        client = create_metrics_client(config)
        return bool(client.ping())
    except Exception as e:
        logger.error("Metrics OpenSearch connection failed: %s", e)
        return False


def _iter_actions(documents: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    for doc in documents:
        index = doc.get("@index")
        doc_id = doc.get("doc_id")
        if not index or not doc_id:
            logger.warning("Skipping doc missing @index or doc_id: %s", doc)
            continue
        source = {k: v for k, v in doc.items() if k != "@index"}
        yield {
            "_op_type": "index",
            "_index": index,
            "_id": doc_id,
            "_source": source,
        }


def bulk_index_documents(
    documents: list[dict[str, Any]],
    config: AttributionConfig,
) -> int:
    """Bulk-index cost attribution documents directly to metrics OpenSearch.

    Uses each document's embedded `@index` (set by schema builders) as the
    target index and `doc_id` as the _id for idempotent upserts.

    Returns:
        Number of documents successfully indexed.
    """
    if not documents:
        logger.info("No documents to index")
        return 0

    client = create_metrics_client(config)
    success, errors = bulk(
        client,
        _iter_actions(documents),
        chunk_size=config.bulk_batch_size,
        raise_on_error=False,
        raise_on_exception=False,
    )

    if errors:
        logger.error(
            "Bulk indexing had %d errors (first: %s)",
            len(errors), errors[0] if errors else None,
        )
        raise RuntimeError(f"Bulk indexing failed for {len(errors)} documents")

    logger.info(
        "Bulk-indexed %d documents to metrics OpenSearch at %s:%d",
        success, config.metrics_host, config.metrics_port,
    )
    return success
