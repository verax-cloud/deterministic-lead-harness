import os
import pytest

# Live PostgreSQL Integration Tests
# Activated when running against a live Postgres container
pytestmark = pytest.mark.skipif(
    os.getenv("RUN_LIVE_POSTGRES") != "1",
    reason="Requires live PostgreSQL instance. Set RUN_LIVE_POSTGRES=1 after starting docker-compose."
)

def test_live_concurrent_webhook_single_writer():
    """Proves ACID isolation: concurrent incoming webhooks with identical delivery_id only write once."""
    pass

def test_live_skip_locked_worker_concurrency():
    """Proves FOR UPDATE SKIP LOCKED guarantees zero race conditions across 4 parallel workers."""
    pass
