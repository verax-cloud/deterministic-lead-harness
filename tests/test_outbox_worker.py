import pytest
import datetime
from src.outbox_worker import OutboxWorker

class MockProviderWithDeduplication:
    """
    Simulates production Resend/SendGrid behavior:
    Caches responses by Idempotency-Key. If identical key arrives,
    returns cached response and increments duplicate_requests_ignored without sending a second email.
    """
    def __init__(self):
        self.sent_emails = []
        self.cached_responses = {}
        self.duplicate_requests_ignored = 0

    def send_email(self, headers, payload):
        idempotency_key = headers.get("Idempotency-Key")
        if idempotency_key in self.cached_responses:
            self.duplicate_requests_ignored += 1
            return self.cached_responses[idempotency_key]

        # First dispatch
        email_record = {"to": payload.get("email"), "idempotency_key": idempotency_key}
        self.sent_emails.append(email_record)
        response = {"status_code": 200, "message_id": f"msg_{len(self.sent_emails)}", "status": "delivered"}
        self.cached_responses[idempotency_key] = response
        return response

class MockCursorWorker:
    def __init__(self):
        self.outbox_row = [1, "lead_42", 1, "outbound_email", '{"email":"test@test.com"}', "outbox_lead_42_1_demo", 0]
        self.status = "PENDING"
        self.processing_started_at = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, query, params=None):
        if "UPDATE transactional_outbox" in query and "status = 'PROCESSING'" in query:
            self.status = "PROCESSING"
            self.processing_started_at = datetime.datetime.now(datetime.timezone.utc)
        elif "UPDATE transactional_outbox" in query and "status = 'COMPLETED'" in query:
            self.status = "COMPLETED"
        elif "UPDATE transactional_outbox" in query and "status = 'FAILED'" in query:
            self.status = "FAILED"

    def fetchone(self):
        if self.status == "COMPLETED":
            return None
        return self.outbox_row

    def fetchall(self):
        if self.status == "PROCESSING":
            return [(1,)]
        return []

class MockConnectionWorker:
    def __init__(self):
        self.cursor_obj = MockCursorWorker()

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass

def test_worker_crash_and_provider_deduplication():
    """Verifies that replaying after a crash sends the identical key and ignores duplicate send."""
    conn = MockConnectionWorker()
    provider = MockProviderWithDeduplication()
    worker = OutboxWorker(conn, provider.send_email)

    # 1. Dispatch succeeds at external provider, but worker crashes before committing COMPLETED
    with pytest.raises(RuntimeError, match="Worker process terminated abruptly"):
        worker.poll_and_dispatch_single(crash_before_commit=True)

    assert len(provider.sent_emails) == 1
    assert provider.duplicate_requests_ignored == 0

    # 2. Worker re-polls upon recovery and resends identical idempotency key
    result = worker.poll_and_dispatch_single(crash_before_commit=False)
    assert result["status"] == "dispatched"

    # Provider cached the response: exactly 1 physical email sent, 1 duplicate ignored
    assert len(provider.sent_emails) == 1
    assert provider.duplicate_requests_ignored == 1

def test_stale_processing_lease_reclaim():
    """Verifies that orphaned PROCESSING rows exceeding lease timeout are reclaimed."""
    conn = MockConnectionWorker()
    provider = MockProviderWithDeduplication()
    worker = OutboxWorker(conn, provider.send_email, lease_timeout_seconds=0)

    conn.cursor_obj.status = "PROCESSING"
    reclaimed = worker.recover_stale_processing()
    assert reclaimed == 1
