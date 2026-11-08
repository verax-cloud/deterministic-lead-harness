import datetime
import json
from typing import Dict, Any, Callable, Optional

RETRY_INTERVALS = [1, 5, 30]

class OutboxWorker:
    def __init__(self, db_connection, mail_provider_client: Callable, lease_timeout_seconds: int = 60):
        self.conn = db_connection
        self.mail_provider = mail_provider_client
        self.lease_timeout_seconds = lease_timeout_seconds

    def recover_stale_processing(self) -> int:
        """
        Reclaims orphaned records stuck in PROCESSING state due to worker crashes.
        Resets records to FAILED so they are scheduled for retry.
        """
        stale_threshold = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=self.lease_timeout_seconds)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE transactional_outbox
                SET status = 'FAILED',
                    retry_count = retry_count + 1,
                    next_retry_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = 'PROCESSING'
                  AND processing_started_at <= %s
                RETURNING id;
                """,
                (stale_threshold,)
            )
            rows = cur.fetchall()
            self.conn.commit()
            return len(rows)

    def poll_and_dispatch_single(self, crash_before_commit: bool = False) -> Dict[str, Any]:
        """
        Polls for eligible PENDING, FAILED, or timed-out PROCESSING rows using SKIP LOCKED.
        """
        with self.conn.cursor() as cur:
            # Query eligible items including expired PROCESSING leases
            cur.execute(
                """
                SELECT id, lead_id, step_version, destination, payload, idempotency_key, retry_count
                FROM transactional_outbox
                WHERE (status IN ('PENDING', 'FAILED') AND next_retry_at <= CURRENT_TIMESTAMP)
                   OR (status = 'PROCESSING' AND processing_started_at <= CURRENT_TIMESTAMP - make_interval(secs => %s))
                ORDER BY id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED;
                """,
                (self.lease_timeout_seconds,)
            )
            row = cur.fetchone()
            if not row:
                return {"status": "empty"}

            outbox_id, lead_id, step_version, destination, payload, idempotency_key, retry_count = row

            cur.execute(
                """
                UPDATE transactional_outbox
                SET status = 'PROCESSING',
                    processing_started_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s;
                """,
                (outbox_id,)
            )
            self.conn.commit()

        headers = {"Idempotency-Key": idempotency_key}
        payload_dict = json.loads(payload) if isinstance(payload, str) else payload

        try:
            response = self.mail_provider(headers=headers, payload=payload_dict)
            if response.get("status_code") == 200:
                if crash_before_commit:
                    raise RuntimeError("Worker process terminated abruptly before commit!")

                with self.conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE transactional_outbox
                        SET status = 'COMPLETED',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                        """,
                        (outbox_id,)
                    )
                    self.conn.commit()
                return {"status": "dispatched", "idempotency_key": idempotency_key, "response": response}
            else:
                raise Exception(f"Provider returned error: {response}")

        except Exception as e:
            self._handle_failure(outbox_id, lead_id, retry_count, payload_dict, str(e))
            return {"status": "failed", "error": str(e)}

    def _handle_failure(self, outbox_id: int, lead_id: str, retry_count: int, payload: Dict[str, Any], reason: str):
        with self.conn.cursor() as cur:
            if retry_count < len(RETRY_INTERVALS):
                next_delay = RETRY_INTERVALS[retry_count]
                next_retry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=next_delay)
                cur.execute(
                    """
                    UPDATE transactional_outbox
                    SET status = 'FAILED',
                        retry_count = retry_count + 1,
                        next_retry_at = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s;
                    """,
                    (next_retry, outbox_id)
                )
            else:
                cur.execute(
                    "UPDATE transactional_outbox SET status = 'DLQ', updated_at = CURRENT_TIMESTAMP WHERE id = %s;",
                    (outbox_id,)
                )
                cur.execute(
                    """
                    INSERT INTO dlq_events (outbox_id, lead_id, failure_reason, payload)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (outbox_id, lead_id, reason, json.dumps(payload))
                )
            self.conn.commit()
