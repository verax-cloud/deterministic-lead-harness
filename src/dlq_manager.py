import json
from typing import List, Dict, Any

class DLQManager:
    def __init__(self, db_connection):
        self.conn = db_connection

    def get_unresolved_events(self) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, outbox_id, lead_id, failure_reason, payload
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
                    "payload": json.loads(r[4]) if isinstance(r[4], str) else r[4]
                }
                for r in rows
            ]

    def replay_event(self, dlq_id: int) -> bool:
        with self.conn.cursor() as cur:
            cur.execute("SELECT outbox_id FROM dlq_events WHERE id = %s", (dlq_id,))
            row = cur.fetchone()
            if not row:
                return False
            outbox_id = row[0]

            cur.execute(
                """
                UPDATE transactional_outbox
                SET status = 'PENDING', retry_count = 0, next_retry_at = CURRENT_TIMESTAMP
                WHERE id = %s;
                """,
                (outbox_id,)
            )
            cur.execute("UPDATE dlq_events SET replayed = TRUE WHERE id = %s;", (dlq_id,))
            self.conn.commit()
            return True
