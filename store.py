import sqlite3
import threading
import time
from typing import List, Optional
from .models import (
    Agent,
    AgentState,
    Borrower,
    Call,
    CallState,
    CALL_STATE_RANK,
    TERMINAL_CALL_STATES,
)


class StateStore:
    """
    SQLite-backed state store using WAL mode and atomic conditional updates
    (Optimistic Concurrency Control / Compare-And-Swap) so multiple workers
    cannot double-allocate agents or borrowers, and duplicate/out-of-order
    events are handled idempotently.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        with self._lock:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS agents (
                    agent_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    current_call_id TEXT,
                    version INTEGER NOT NULL DEFAULT 1,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS borrowers (
                    borrower_id TEXT PRIMARY KEY,
                    phone_number TEXT NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    version INTEGER NOT NULL DEFAULT 1,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS calls (
                    call_id TEXT PRIMARY KEY,
                    borrower_id TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    agent_id TEXT,
                    provider_call_id TEXT UNIQUE,
                    provider_name TEXT,
                    state TEXT NOT NULL,
                    worker_id TEXT,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    answered_at REAL,
                    connected_at REAL,
                    ended_at REAL,
                    abandoned INTEGER NOT NULL DEFAULT 0,
                    failure_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id TEXT PRIMARY KEY,
                    provider_call_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    processed_at REAL NOT NULL
                );
                """
            )

    # --- Agent Operations ---

    def add_agent(self, agent: Agent):
        with self._lock:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO agents (agent_id, name, state, current_call_id, version, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (agent.agent_id, agent.name, agent.state.value, agent.current_call_id, agent.version, agent.updated_at),
            )

    def get_agent(self, agent_id: str) -> Optional[Agent]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
            return self._row_to_agent(row) if row else None

    def list_agents(self, state: Optional[AgentState] = None) -> List[Agent]:
        with self._lock:
            if state:
                rows = self.conn.execute("SELECT * FROM agents WHERE state = ?", (state.value,)).fetchall()
            else:
                rows = self.conn.execute("SELECT * FROM agents").fetchall()
            return [self._row_to_agent(r) for r in rows]

    def count_agents_by_state(self) -> dict:
        with self._lock:
            counts = {s: 0 for s in AgentState}
            rows = self.conn.execute("SELECT state, COUNT(*) as cnt FROM agents GROUP BY state").fetchall()
            for r in rows:
                counts[AgentState(r["state"])] = r["cnt"]
            return counts

    def atomic_reserve_agent(self, call_id: str, specific_agent_id: Optional[str] = None) -> Optional[Agent]:
        """
        Atomically claims ONE AVAILABLE agent and moves them to RESERVED.
        Guarantees zero double-allocation across concurrent workers.
        """
        now = time.time()
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE;")
            try:
                if specific_agent_id:
                    row = self.conn.execute(
                        "SELECT * FROM agents WHERE agent_id = ? AND state = ?",
                        (specific_agent_id, AgentState.AVAILABLE.value),
                    ).fetchone()
                else:
                    row = self.conn.execute(
                        "SELECT * FROM agents WHERE state = ? ORDER BY updated_at ASC LIMIT 1",
                        (AgentState.AVAILABLE.value,),
                    ).fetchone()

                if not row:
                    self.conn.execute("COMMIT;")
                    return None

                cur = self.conn.execute(
                    """
                    UPDATE agents
                    SET state = ?, current_call_id = ?, version = version + 1, updated_at = ?
                    WHERE agent_id = ? AND state = ? AND version = ?
                    """,
                    (
                        AgentState.RESERVED.value,
                        call_id,
                        now,
                        row["agent_id"],
                        AgentState.AVAILABLE.value,
                        row["version"],
                    ),
                )
                self.conn.execute("COMMIT;")
                if cur.rowcount == 1:
                    return self.get_agent(row["agent_id"])
                return None
            except Exception:
                self.conn.execute("ROLLBACK;")
                raise

    def transition_agent(
        self,
        agent_id: str,
        to_state: AgentState,
        expected_states: Optional[List[AgentState]] = None,
        call_id: Optional[str] = None,
        clear_call: bool = False,
    ) -> bool:
        now = time.time()
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE;")
            try:
                row = self.conn.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
                if not row:
                    self.conn.execute("COMMIT;")
                    return False

                current_state = AgentState(row["state"])
                if expected_states and current_state not in expected_states:
                    self.conn.execute("COMMIT;")
                    return False

                new_call_id = None if clear_call else (call_id if call_id is not None else row["current_call_id"])
                cur = self.conn.execute(
                    """
                    UPDATE agents
                    SET state = ?, current_call_id = ?, version = version + 1, updated_at = ?
                    WHERE agent_id = ? AND version = ?
                    """,
                    (to_state.value, new_call_id, now, agent_id, row["version"]),
                )
                self.conn.execute("COMMIT;")
                return cur.rowcount == 1
            except Exception:
                self.conn.execute("ROLLBACK;")
                raise

    # --- Borrower Operations ---

    def add_borrower(self, borrower: Borrower):
        with self._lock:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO borrowers (borrower_id, phone_number, name, status, attempts, max_attempts, version, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    borrower.borrower_id,
                    borrower.phone_number,
                    borrower.name,
                    borrower.status,
                    borrower.attempts,
                    borrower.max_attempts,
                    borrower.version,
                    borrower.updated_at,
                ),
            )

    def get_borrower(self, borrower_id: str) -> Optional[Borrower]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM borrowers WHERE borrower_id = ?", (borrower_id,)).fetchone()
            return self._row_to_borrower(row) if row else None

    def count_pending_borrowers(self) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM borrowers WHERE status IN ('PENDING', 'RETRY') AND attempts < max_attempts"
            ).fetchone()
            return row["cnt"]

    def atomic_claim_borrower(self) -> Optional[Borrower]:
        """Atomically claims a borrower so two workers never dial the same borrower simultaneously."""
        now = time.time()
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE;")
            try:
                row = self.conn.execute(
                    """
                    SELECT * FROM borrowers
                    WHERE status IN ('PENDING', 'RETRY') AND attempts < max_attempts
                    ORDER BY updated_at ASC LIMIT 1
                    """
                ).fetchone()
                if not row:
                    self.conn.execute("COMMIT;")
                    return None

                cur = self.conn.execute(
                    """
                    UPDATE borrowers
                    SET status = 'RESERVED', attempts = attempts + 1, version = version + 1, updated_at = ?
                    WHERE borrower_id = ? AND version = ?
                    """,
                    (now, row["borrower_id"], row["version"]),
                )
                self.conn.execute("COMMIT;")
                if cur.rowcount == 1:
                    return self.get_borrower(row["borrower_id"])
                return None
            except Exception:
                self.conn.execute("ROLLBACK;")
                raise

    def update_borrower_status(self, borrower_id: str, status: str):
        now = time.time()
        with self._lock:
            self.conn.execute(
                "UPDATE borrowers SET status = ?, version = version + 1, updated_at = ? WHERE borrower_id = ?",
                (status, now, borrower_id),
            )

    # --- Call Operations ---

    def create_call(self, call: Call):
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO calls (
                    call_id, borrower_id, mode, agent_id, provider_call_id, provider_name,
                    state, worker_id, version, created_at, updated_at, answered_at,
                    connected_at, ended_at, abandoned, failure_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    call.call_id,
                    call.borrower_id,
                    call.mode,
                    call.agent_id,
                    call.provider_call_id,
                    call.provider_name,
                    call.state.value,
                    call.worker_id,
                    call.version,
                    call.created_at,
                    call.updated_at,
                    call.answered_at,
                    call.connected_at,
                    call.ended_at,
                    1 if call.abandoned else 0,
                    call.failure_reason,
                ),
            )

    def get_call(self, call_id: str) -> Optional[Call]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM calls WHERE call_id = ?", (call_id,)).fetchone()
            return self._row_to_call(row) if row else None

    def get_call_by_provider_id(self, provider_call_id: str) -> Optional[Call]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM calls WHERE provider_call_id = ?", (provider_call_id,)
            ).fetchone()
            return self._row_to_call(row) if row else None

    def list_calls(self, states: Optional[List[CallState]] = None) -> List[Call]:
        with self._lock:
            if states:
                placeholders = ",".join("?" for _ in states)
                rows = self.conn.execute(
                    f"SELECT * FROM calls WHERE state IN ({placeholders})", [s.value for s in states]
                ).fetchall()
            else:
                rows = self.conn.execute("SELECT * FROM calls").fetchall()
            return [self._row_to_call(r) for r in rows]

    def is_event_processed(self, event_id: str) -> bool:
        with self._lock:
            row = self.conn.execute(
                "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
            ).fetchone()
            return row is not None

    def record_event(self, event_id: str, provider_call_id: str, event_type: str) -> bool:
        """Returns True if newly recorded, False if duplicate."""
        with self._lock:
            try:
                self.conn.execute(
                    """
                    INSERT INTO processed_events (event_id, provider_call_id, event_type, processed_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (event_id, provider_call_id, event_type, time.time()),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def advance_call_state(
        self,
        call_id: str,
        target_state: CallState,
        agent_id: Optional[str] = None,
        provider_call_id: Optional[str] = None,
        provider_name: Optional[str] = None,
        abandoned: Optional[bool] = None,
        failure_reason: Optional[str] = None,
    ) -> Optional[Call]:
        """
        Monotonic state transition guard:
        - Never regresses an already-advanced or terminal call state (protects against out-of-order events).
        - Uses optimistic locking (version check) for concurrent safety.
        """
        now = time.time()
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE;")
            try:
                row = self.conn.execute("SELECT * FROM calls WHERE call_id = ?", (call_id,)).fetchone()
                if not row:
                    self.conn.execute("COMMIT;")
                    return None

                current_state = CallState(row["state"])

                # Ignore out-of-order or post-terminal transitions
                if current_state in TERMINAL_CALL_STATES:
                    self.conn.execute("COMMIT;")
                    return self._row_to_call(row)

                if CALL_STATE_RANK[target_state] < CALL_STATE_RANK[current_state]:
                    self.conn.execute("COMMIT;")
                    return self._row_to_call(row)

                if target_state == current_state and provider_call_id is None and agent_id is None:
                    self.conn.execute("COMMIT;")
                    return self._row_to_call(row)

                new_agent_id = agent_id if agent_id is not None else row["agent_id"]
                new_prov_id = provider_call_id if provider_call_id is not None else row["provider_call_id"]
                new_prov_name = provider_name if provider_name is not None else row["provider_name"]
                new_abandoned = (1 if abandoned else 0) if abandoned is not None else row["abandoned"]
                new_reason = failure_reason if failure_reason is not None else row["failure_reason"]

                answered_at = row["answered_at"]
                connected_at = row["connected_at"]
                ended_at = row["ended_at"]

                if target_state == CallState.ANSWERED and answered_at is None:
                    answered_at = now
                if target_state == CallState.CONNECTED and connected_at is None:
                    connected_at = now
                if target_state in TERMINAL_CALL_STATES and ended_at is None:
                    ended_at = now

                cur = self.conn.execute(
                    """
                    UPDATE calls
                    SET state = ?, agent_id = ?, provider_call_id = ?, provider_name = ?,
                        answered_at = ?, connected_at = ?, ended_at = ?, abandoned = ?,
                        failure_reason = ?, version = version + 1, updated_at = ?
                    WHERE call_id = ? AND version = ?
                    """,
                    (
                        target_state.value,
                        new_agent_id,
                        new_prov_id,
                        new_prov_name,
                        answered_at,
                        connected_at,
                        ended_at,
                        new_abandoned,
                        new_reason,
                        now,
                        call_id,
                        row["version"],
                    ),
                )
                self.conn.execute("COMMIT;")
                if cur.rowcount == 1:
                    return self.get_call(call_id)
                return self.get_call(call_id)
            except Exception:
                self.conn.execute("ROLLBACK;")
                raise

    # --- Row Mappers ---

    @staticmethod
    def _row_to_agent(row: sqlite3.Row) -> Agent:
        return Agent(
            agent_id=row["agent_id"],
            name=row["name"],
            state=AgentState(row["state"]),
            current_call_id=row["current_call_id"],
            version=row["version"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_borrower(row: sqlite3.Row) -> Borrower:
        return Borrower(
            borrower_id=row["borrower_id"],
            phone_number=row["phone_number"],
            name=row["name"],
            status=row["status"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            version=row["version"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_call(row: sqlite3.Row) -> Call:
        return Call(
            call_id=row["call_id"],
            borrower_id=row["borrower_id"],
            mode=row["mode"],
            agent_id=row["agent_id"],
            provider_call_id=row["provider_call_id"],
            provider_name=row["provider_name"],
            state=CallState(row["state"]),
            worker_id=row["worker_id"],
            version=row["version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            answered_at=row["answered_at"],
            connected_at=row["connected_at"],
            ended_at=row["ended_at"],
            abandoned=bool(row["abandoned"]),
            failure_reason=row["failure_reason"],
        )
