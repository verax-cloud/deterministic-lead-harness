import datetime
import json
from typing import List, Dict, Any

class DLQManager:
    def __init__(self, db_connection, max_auto_replays: int = 3):
        self.conn = db_connection
        self.max_auto_replays = max_auto_replays

    def get_unresolved_events(self) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, outbox_id, lead_id, failure_reason, payload, replay_count
                FROM dlq_events
                WHERE replayed = FALSE AND escalated_to_human = FALSE;
                """
            )
            rows = cur.fetchall()
            return [
                {
                    "dlq_id": r[0],
                    "outbox_id": r[1],
                    "lead_id": r[2],
                    "reason": r[3],
                    "payload": json.loads(r[4]) if isinstance(r[4], str) else r[4],
                    "replay_count": r[5]
                }
                for r in rows
            ]

    def evaluate_and_replay_dlq(self, interval_minutes: int = 5) -> Dict[str, int]:
        """
        Automated 5-minute background loop:
        1. Checks events older than interval_minutes.
        2. If replay_count < max_auto_replays, resets outbox record to PENDING.
        3. If replay_count >= max_auto_replays, escalates event to human intervention.
        """
        cutoff_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=interval_minutes)
        replayed_count = 0
        escalated_count = 0

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, outbox_id, replay_count
                FROM dlq_events
                WHERE replayed = FALSE
                  AND escalated_to_human = FALSE
                  AND created_at <= %s;
                """,
                (cutoff_time,)
            )
            events = cur.fetchall()

            for dlq_id, outbox_id, replay_count in events:
                if replay_count < self.max_auto_replays:
                    cur.execute(
                        """
                        UPDATE transactional_outbox
                        SET status = 'PENDING',
                            retry_count = 0,
                            next_retry_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                        """,
                        (outbox_id,)
                    )
                    cur.execute(
                        """
                        UPDATE dlq_events
                        SET replayed = TRUE,
                            replay_count = replay_count + 1,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                        """,
                        (dlq_id,)
                    )
                    replayed_count += 1
                else:
                    cur.execute(
                        """
                        UPDATE dlq_events
                        SET escalated_to_human = TRUE,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                        """,
                        (dlq_id,)
                    )
                    escalated_count += 1

            self.conn.commit()

        return {"replayed": replayed_count, "escalated": escalated_count}

    def manual_replay_event(self, dlq_id: int) -> bool:
        with self.conn.cursor() as cur:
            cur.execute("SELECT outbox_id FROM dlq_events WHERE id = %s", (dlq_id,))
            row = cur.fetchone()
            if not row:
                return False
            outbox_id = row[0]

            cur.execute(
                """
                UPDATE transactional_outbox
                SET status = 'PENDING',
                    retry_count = 0,
                    next_retry_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s;
                """,
                (outbox_id,)
            )
            cur.execute("UPDATE dlq_events SET replayed = TRUE, updated_at = CURRENT_TIMESTAMP WHERE id = %s;", (dlq_id,))
            self.conn.commit()
            return True
