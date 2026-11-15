"""Swarm SQLite WAL State Layer.

Provides thread-safe and process-safe SQLite WAL persistence for swarms,
members, and monotonic message board logs with lifecycle state machine validation.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

VALID_STATES: frozenset[str] = frozenset({"working", "waiting", "stopped", "killed"})
TERMINAL_STATES: frozenset[str] = frozenset({"killed"})

VALID_TRANSITIONS: dict[str, frozenset[str]] = {
    "waiting": frozenset({"working", "stopped", "killed"}),
    "working": frozenset({"waiting", "stopped", "killed"}),
    "stopped": frozenset({"waiting", "working", "killed"}),
    "killed": frozenset(),  # Terminal: no outbound transitions permitted
}

VALID_WORKSPACE_MODES: frozenset[str] = frozenset({"inherit", "share", "branch"})

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS swarms (
    swarm_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    coordinator_sid TEXT NOT NULL,
    workspace_root TEXT NOT NULL,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS swarm_members (
    swarm_id TEXT NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'worker',
    pool_name TEXT,
    brief TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    provider TEXT NOT NULL DEFAULT '',
    reasoning_effort TEXT,
    workspace_mode TEXT NOT NULL DEFAULT 'inherit',
    workspace_path TEXT NOT NULL DEFAULT '',
    branch_name TEXT,
    session_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'waiting',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (swarm_id, name),
    FOREIGN KEY (swarm_id) REFERENCES swarms(swarm_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS swarm_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    swarm_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    author TEXT NOT NULL,
    mentions TEXT NOT NULL DEFAULT '[]',
    text TEXT NOT NULL,
    hop_count INTEGER NOT NULL DEFAULT 0,
    timestamp TEXT NOT NULL,
    UNIQUE(swarm_id, seq),
    FOREIGN KEY (swarm_id) REFERENCES swarms(swarm_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_messages_swarm_seq ON swarm_messages(swarm_id, seq);
CREATE INDEX IF NOT EXISTS idx_members_swarm_state ON swarm_members(swarm_id, state);
CREATE INDEX IF NOT EXISTS idx_members_swarm_pool ON swarm_members(swarm_id, pool_name);
"""

# Connection-level lock cache to safely serialize concurrent operations
# if a single sqlite3.Connection handle is shared across threads.
_CONN_LOCKS: dict[int, threading.RLock] = {}
_CONN_LOCKS_GUARD = threading.Lock()


def _get_conn_lock(conn: sqlite3.Connection) -> threading.RLock:
    conn_id = id(conn)
    with _CONN_LOCKS_GUARD:
        lock = _CONN_LOCKS.get(conn_id)
        if lock is None:
            lock = threading.RLock()
            _CONN_LOCKS[conn_id] = lock
        return lock


def _utc_now_iso() -> str:
    """Return current UTC timestamp formatted as ISO-8601 with timezone indicator."""
    return datetime.now(timezone.utc).isoformat()


def init_swarm_db(db_path: Path | str) -> sqlite3.Connection:
    """Open or create a Swarm SQLite database configured with WAL mode and tables.

    Parameters:
        db_path: Path to the SQLite database file or ':memory:'.

    Returns:
        sqlite3.Connection configured with Row factory, busy timeout, and WAL mode.
    """
    path_str = str(db_path)
    if path_str != ":memory:":
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        db_target = str(path)
    else:
        db_target = ":memory:"

    conn = sqlite3.connect(
        db_target,
        timeout=5.0,
        check_same_thread=False,
        isolation_level=None,  # Explicit transaction management
    )
    conn.row_factory = sqlite3.Row

    # Execute connection-level PRAGMAs
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")

    # Initialize tables and indices
    conn.executescript(SCHEMA_SQL)
    return conn


@contextlib.contextmanager
def write_txn(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Context manager executing BEGIN IMMEDIATE for exclusive write reservations.

    Commits on clean exit, rolls back on exception. Supports nested re-entrant
    calls via SQLite SAVEPOINT without deadlocks.
    """
    lock = _get_conn_lock(conn)
    with lock:
        if conn.in_transaction:
            # Re-entrant transaction: use SAVEPOINT
            sp_name = f"sp_{uuid.uuid4().hex[:8]}"
            conn.execute(f"SAVEPOINT {sp_name}")
            try:
                yield conn
                conn.execute(f"RELEASE SAVEPOINT {sp_name}")
            except BaseException:
                try:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                    conn.execute(f"RELEASE SAVEPOINT {sp_name}")
                except sqlite3.OperationalError:
                    pass
                raise
            return

        prev_iso = conn.isolation_level
        if prev_iso is not None:
            conn.isolation_level = None

        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass  # Guard against SQLite auto-rollback
            raise
        finally:
            if prev_iso is not None:
                conn.isolation_level = prev_iso


def create_swarm(
    conn: sqlite3.Connection,
    swarm_id: str,
    title: str,
    coordinator_sid: str,
    workspace_root: str,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a new swarm record.

    Raises:
        ValueError: If swarm_id or title is empty, or if swarm_id already exists.

    Returns:
        Dict representing the created swarm record.
    """
    if not swarm_id or not str(swarm_id).strip():
        raise ValueError("swarm_id cannot be empty")
    if not title or not str(title).strip():
        raise ValueError("title cannot be empty")

    now_ts = _utc_now_iso()
    config_dict = config if config is not None else {}
    config_json = json.dumps(config_dict)

    with write_txn(conn):
        existing = conn.execute(
            "SELECT 1 FROM swarms WHERE swarm_id = ?", (swarm_id,)
        ).fetchone()
        if existing:
            raise ValueError(f"Swarm '{swarm_id}' already exists")
        conn.execute(
            """INSERT INTO swarms (swarm_id, title, coordinator_sid, workspace_root, config, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (swarm_id, title, coordinator_sid, str(workspace_root), config_json, now_ts, now_ts),
        )

    return {
        "swarm_id": swarm_id,
        "title": title,
        "coordinator_sid": coordinator_sid,
        "workspace_root": str(workspace_root),
        "config": config_dict,
        "created_at": now_ts,
        "updated_at": now_ts,
    }


def get_swarm(conn: sqlite3.Connection, swarm_id: str) -> dict[str, Any] | None:
    """Retrieve swarm metadata by swarm_id.

    Returns:
        Swarm dict if found (config deserialized to dict), else None.
    """
    row = conn.execute("SELECT * FROM swarms WHERE swarm_id = ?", (swarm_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    raw_config = d.get("config")
    if isinstance(raw_config, str):
        try:
            d["config"] = json.loads(raw_config)
        except Exception:
            d["config"] = {}
    elif raw_config is None:
        d["config"] = {}
    return d


def add_member(
    conn: sqlite3.Connection,
    swarm_id: str,
    name: str,
    role: str = "worker",
    pool_name: str | None = None,
    brief: str = "",
    model: str = "",
    provider: str = "",
    reasoning_effort: str | None = None,
    workspace_mode: str = "inherit",
    workspace_path: str = "",
    branch_name: str | None = None,
    session_id: str = "",
    initial_state: str = "waiting",
) -> dict[str, Any]:
    """Add a member to an existing swarm.

    Raises:
        ValueError: If swarm does not exist, name is empty, workspace_mode is
                    invalid, initial_state is killed/invalid, or member name
                    already exists in the swarm.

    Returns:
        Dict representing the inserted member record.
    """
    if not swarm_id or not str(swarm_id).strip():
        raise ValueError("swarm_id cannot be empty")
    if not name or not str(name).strip():
        raise ValueError("name cannot be empty")
    if workspace_mode not in VALID_WORKSPACE_MODES:
        raise ValueError(
            f"Invalid workspace_mode '{workspace_mode}'. Valid modes: {sorted(list(VALID_WORKSPACE_MODES))}"
        )
    allowed_initial_states = VALID_STATES - TERMINAL_STATES
    if initial_state in TERMINAL_STATES or initial_state not in VALID_STATES:
        raise ValueError(
            f"Invalid initial state '{initial_state}'. Must be one of: {sorted(list(allowed_initial_states))}"
        )

    now_ts = _utc_now_iso()

    with write_txn(conn):
        swarm = conn.execute("SELECT 1 FROM swarms WHERE swarm_id = ?", (swarm_id,)).fetchone()
        if not swarm:
            raise ValueError(f"Swarm '{swarm_id}' does not exist")

        existing = conn.execute(
            "SELECT 1 FROM swarm_members WHERE swarm_id = ? AND name = ?",
            (swarm_id, name),
        ).fetchone()
        if existing:
            raise ValueError(f"Member '{name}' already exists in swarm '{swarm_id}'")

        conn.execute(
            """INSERT INTO swarm_members
               (swarm_id, name, role, pool_name, brief, model, provider, reasoning_effort,
                workspace_mode, workspace_path, branch_name, session_id, state, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                swarm_id,
                name,
                role,
                pool_name,
                brief,
                model,
                provider,
                reasoning_effort,
                workspace_mode,
                str(workspace_path),
                branch_name,
                session_id,
                initial_state,
                now_ts,
                now_ts,
            ),
        )

    return {
        "swarm_id": swarm_id,
        "name": name,
        "role": role,
        "pool_name": pool_name,
        "brief": brief,
        "model": model,
        "provider": provider,
        "reasoning_effort": reasoning_effort,
        "workspace_mode": workspace_mode,
        "workspace_path": str(workspace_path),
        "branch_name": branch_name,
        "session_id": session_id,
        "state": initial_state,
        "created_at": now_ts,
        "updated_at": now_ts,
    }


def get_member(conn: sqlite3.Connection, swarm_id: str, name: str) -> dict[str, Any] | None:
    """Retrieve a single member of a swarm by name.

    Returns:
        Member dict if found, else None.
    """
    row = conn.execute(
        "SELECT * FROM swarm_members WHERE swarm_id = ? AND name = ?",
        (swarm_id, name),
    ).fetchone()
    return dict(row) if row else None


def get_members(conn: sqlite3.Connection, swarm_id: str) -> list[dict[str, Any]]:
    """Retrieve all members of a swarm.

    Returns:
        List of member dicts ordered by created_at ASC, name ASC.
    """
    rows = conn.execute(
        "SELECT * FROM swarm_members WHERE swarm_id = ? ORDER BY created_at ASC, name ASC",
        (swarm_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def update_member_state(
    conn: sqlite3.Connection,
    swarm_id: str,
    name: str,
    new_state: str,
) -> None:
    """Validate and transition a member's lifecycle state.

    Raises:
        ValueError: If member not found, new_state is invalid, worker is in
                    terminal state 'killed', or transition is illegal.
    """
    if new_state not in VALID_STATES:
        raise ValueError(
            f"Invalid member state '{new_state}'. Valid states are: {sorted(list(VALID_STATES))}"
        )

    with write_txn(conn):
        row = conn.execute(
            "SELECT state FROM swarm_members WHERE swarm_id = ? AND name = ?",
            (swarm_id, name),
        ).fetchone()
        if not row:
            raise ValueError(f"Member '{name}' not found in swarm '{swarm_id}'")

        curr_state = row["state"]

        if curr_state in TERMINAL_STATES:
            raise ValueError(
                f"Worker '{name}' is in terminal state '{curr_state}' and cannot transition to '{new_state}'"
            )

        if curr_state == new_state:
            # Idempotent self-transition for active states
            return

        legal_targets = VALID_TRANSITIONS.get(curr_state, frozenset())
        if new_state not in legal_targets:
            raise ValueError(
                f"Illegal transition for member '{name}' from '{curr_state}' to '{new_state}'"
            )

        now_ts = _utc_now_iso()
        conn.execute(
            "UPDATE swarm_members SET state = ?, updated_at = ? WHERE swarm_id = ? AND name = ?",
            (new_state, now_ts, swarm_id, name),
        )


def append_message(
    conn: sqlite3.Connection,
    swarm_id: str,
    author: str,
    mentions: list[str] | None,
    text: str,
    hop_count: int = 0,
) -> int:
    """Atomically append a message with strictly monotonic sequence number.

    Computes:
        seq = SELECT COALESCE(MAX(seq), 0) + 1 FROM swarm_messages WHERE swarm_id = ?
    under write_txn.

    Raises:
        ValueError: If swarm does not exist, swarm_id or author is empty, or hop_count < 0.

    Returns:
        Assigned integer sequence number (seq).
    """
    if not swarm_id or not str(swarm_id).strip():
        raise ValueError("swarm_id cannot be empty")
    if not author or not str(author).strip():
        raise ValueError("author cannot be empty")
    if hop_count < 0:
        raise ValueError("hop_count cannot be negative")

    mentions_list = list(mentions) if mentions is not None else []
    mentions_json = json.dumps(mentions_list)
    now_ts = _utc_now_iso()

    with write_txn(conn):
        swarm = conn.execute("SELECT 1 FROM swarms WHERE swarm_id = ?", (swarm_id,)).fetchone()
        if not swarm:
            raise ValueError(f"Swarm '{swarm_id}' does not exist")

        cur = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM swarm_messages WHERE swarm_id = ?",
            (swarm_id,),
        )
        row = cur.fetchone()
        seq = int(row[0]) if row and row[0] is not None else 1

        conn.execute(
            """INSERT INTO swarm_messages (swarm_id, seq, author, mentions, text, hop_count, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (swarm_id, seq, author, mentions_json, text, hop_count, now_ts),
        )
        return seq


def get_messages(
    conn: sqlite3.Connection,
    swarm_id: str,
    since_seq: int = 0,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Retrieve messages for a swarm with seq > since_seq ordered by seq ASC.

    Returns:
        List of deserialized message dicts (mentions deserialized to list[str]).
    """
    rows = conn.execute(
        """SELECT id, swarm_id, seq, author, mentions, text, hop_count, timestamp
           FROM swarm_messages
           WHERE swarm_id = ? AND seq > ?
           ORDER BY seq ASC
           LIMIT ?""",
        (swarm_id, since_seq, limit),
    ).fetchall()

    results: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        raw_mentions = d.get("mentions")
        if isinstance(raw_mentions, str):
            try:
                d["mentions"] = json.loads(raw_mentions)
            except Exception:
                d["mentions"] = []
        elif raw_mentions is None:
            d["mentions"] = []
        results.append(d)
    return results
