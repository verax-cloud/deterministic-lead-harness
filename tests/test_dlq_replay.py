import pytest
from src.outbox_worker import OutboxWorker
from src.dlq_manager import DLQManager

class FailingProvider:
    def send_email(self, headers, payload):
        return {"status_code": 500, "error": "Internal upstream service error"}

class MockCursorDLQ:
    def __init__(self):
        self.retry_count = 0
        self.status = "PENDING"
        self.dlq_entries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, query, params=None):
        if "UPDATE transactional_outbox" in query and "retry_count = retry_count + 1" in query:
            self.retry_count += 1
            self.status = "FAILED"
        elif "status = 'DLQ'" in query:
            self.status = "DLQ"
        elif "INSERT INTO dlq_events" in query:
            self.dlq_entries.append([1, params[0], params[1], params[2], params[3], False, 0, False])
        elif "UPDATE dlq_events" in query and "replayed = TRUE" in query:
            if self.dlq_entries:
                self.dlq_entries[0][5] = True
                self.dlq_entries[0][6] += 1
        elif "UPDATE dlq_events" in query and "escalated_to_human = TRUE" in query:
            if self.dlq_entries:
                self.dlq_entries[0][7] = True

    def fetchone(self):
        if self.status in ["PENDING", "FAILED"]:
            return (1, "lead_77", 1, "outbound_email", '{"email":"lead77@test.com"}', "outbox_lead_77_1", self.retry_count)
        return None

    def fetchall(self):
        # Return pending DLQ entries
        return [
            (entry[0], entry[1], entry[6])
            for entry in self.dlq_entries
            if not entry[5] and not entry[7]
        ]

class MockConnectionDLQ:
    def __init__(self):
        self.cursor_obj = MockCursorDLQ()

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass

def test_automated_5min_dlq_evaluator_loop():
    conn = MockConnectionDLQ()
    failing_provider = FailingProvider()
    worker = OutboxWorker(conn, failing_provider.send_email)
    dlq_mgr = DLQManager(conn, max_auto_replays=2)

    # Exhaust 3 worker retries -> enters DLQ
    for _ in range(4):
        worker.poll_and_dispatch_single()

    assert conn.cursor_obj.status == "DLQ"
    assert len(conn.cursor_obj.dlq_entries) == 1

    # Automated 5-min evaluation loop triggers replay
    result = dlq_mgr.evaluate_and_replay_dlq(interval_minutes=0)
    assert result["replayed"] == 1
    assert result["escalated"] == 0
    assert conn.cursor_obj.dlq_entries[0][5] is True
