"""Unit tests for Swarm SQLite WAL State Layer (plugins/swarm/state.py).

Verifies PRAGMAs, schema initialization, idempotent migrations, CRUD helpers,
state machine transitions, strict terminal behavior of 'killed', monotonic sequence
generation under heavy concurrency, pagination, savepoints, and transactions.
"""

from __future__ import annotations

import concurrent.futures
import sqlite3
import threading
from pathlib import Path
from typing import Any

import pytest

from plugins.swarm.state import (
    TERMINAL_STATES,
    VALID_STATES,
    VALID_TRANSITIONS,
    VALID_WORKSPACE_MODES,
    add_member,
    append_message,
    create_swarm,
    get_member,
    get_members,
    get_messages,
    get_swarm,
    init_swarm_db,
    update_member_state,
    write_txn,
)


# ==============================================================================
# Database Initialization & PRAGMAs
# ==============================================================================


def test_init_swarm_db_pragmas_and_schema(tmp_path: Path) -> None:
    db_file = tmp_path / "test_swarm.db"
    conn = init_swarm_db(db_file)

    # 1. PRAGMA journal_mode = WAL
    row = conn.execute("PRAGMA journal_mode;").fetchone()
    assert row is not None
    assert str(row[0]).lower() == "wal"

    # 2. PRAGMA busy_timeout = 5000
    row = conn.execute("PRAGMA busy_timeout;").fetchone()
    assert row is not None
    assert int(row[0]) >= 5000

    # 3. PRAGMA synchronous = NORMAL (1)
    row = conn.execute("PRAGMA synchronous;").fetchone()
    assert row is not None
    assert int(row[0]) == 1

    # 4. PRAGMA foreign_keys = ON (1)
    row = conn.execute("PRAGMA foreign_keys;").fetchone()
    assert row is not None
    assert int(row[0]) == 1

    # 5. Tables exist
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "swarms" in tables
    assert "swarm_members" in tables
    assert "swarm_messages" in tables

    # 6. Indices exist
    indices = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }
    assert "idx_messages_swarm_seq" in indices
    assert "idx_members_swarm_state" in indices
    assert "idx_members_swarm_pool" in indices

    conn.close()


def test_init_swarm_db_idempotent(tmp_path: Path) -> None:
    db_file = tmp_path / "idempotent.db"
    conn1 = init_swarm_db(db_file)
    create_swarm(conn1, "s1", "Title 1", "coord_1", str(tmp_path))
    conn1.close()

    # Re-open multiple times
    conn2 = init_swarm_db(db_file)
    swarm = get_swarm(conn2, "s1")
    assert swarm is not None
    assert swarm["title"] == "Title 1"

    # Re-run init_swarm_db on same connection/path
    conn3 = init_swarm_db(db_file)
    swarm2 = get_swarm(conn3, "s1")
    assert swarm2 is not None
    assert swarm2["title"] == "Title 1"

    conn2.close()
    conn3.close()


def test_init_swarm_db_memory() -> None:
    conn = init_swarm_db(":memory:")
    create_swarm(conn, "mem_swarm", "Memory Swarm", "coord_sid", "/workspace")
    swarm = get_swarm(conn, "mem_swarm")
    assert swarm is not None
    assert swarm["swarm_id"] == "mem_swarm"
    conn.close()


# ==============================================================================
# Swarm CRUD & Validation
# ==============================================================================


def test_create_and_get_swarm(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "swarms.db")
    config = {
        "max_turns": 10,
        "features": ["code", "review"],
        "nested": {"enabled": True, "count": 42},
    }

    res = create_swarm(
        conn,
        swarm_id="swarm_alpha",
        title="Alpha Swarm",
        coordinator_sid="session_xyz",
        workspace_root=str(tmp_path / "work"),
        config=config,
    )

    assert res["swarm_id"] == "swarm_alpha"
    assert res["title"] == "Alpha Swarm"
    assert res["coordinator_sid"] == "session_xyz"
    assert res["workspace_root"] == str(tmp_path / "work")
    assert res["config"] == config
    assert "created_at" in res
    assert "updated_at" in res

    # Retrieve via get_swarm
    retrieved = get_swarm(conn, "swarm_alpha")
    assert retrieved is not None
    assert retrieved["swarm_id"] == "swarm_alpha"
    assert retrieved["title"] == "Alpha Swarm"
    assert retrieved["coordinator_sid"] == "session_xyz"
    assert retrieved["workspace_root"] == str(tmp_path / "work")
    assert retrieved["config"] == config
    assert retrieved["created_at"] == res["created_at"]
    assert retrieved["updated_at"] == res["updated_at"]

    conn.close()


def test_create_swarm_validation(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "validation.db")

    # Empty swarm_id
    with pytest.raises(ValueError, match="swarm_id cannot be empty"):
        create_swarm(conn, "", "Title", "coord", str(tmp_path))

    with pytest.raises(ValueError, match="swarm_id cannot be empty"):
        create_swarm(conn, "   ", "Title", "coord", str(tmp_path))

    # Empty title
    with pytest.raises(ValueError, match="title cannot be empty"):
        create_swarm(conn, "s1", "", "coord", str(tmp_path))

    # Duplicate swarm_id
    create_swarm(conn, "s1", "Title 1", "coord", str(tmp_path))
    with pytest.raises(ValueError, match="Swarm 's1' already exists"):
        create_swarm(conn, "s1", "Duplicate Title", "coord", str(tmp_path))

    # Nonexistent swarm retrieval
    assert get_swarm(conn, "nonexistent") is None

    conn.close()


# ==============================================================================
# Member CRUD & Validation
# ==============================================================================


def test_add_and_get_member(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "members.db")
    create_swarm(conn, "swarm_m", "Member Test", "coord_1", str(tmp_path))

    member = add_member(
        conn,
        swarm_id="swarm_m",
        name="coder-1",
        role="worker",
        pool_name="coders",
        brief="Write unit tests",
        model="gemini-2.5-pro",
        provider="antigravity",
        reasoning_effort="high",
        workspace_mode="branch",
        workspace_path=str(tmp_path / "work" / "coder-1"),
        branch_name="feature/coder-1",
        session_id="sid_coder_1",
        initial_state="waiting",
    )

    assert member["swarm_id"] == "swarm_m"
    assert member["name"] == "coder-1"
    assert member["role"] == "worker"
    assert member["pool_name"] == "coders"
    assert member["brief"] == "Write unit tests"
    assert member["model"] == "gemini-2.5-pro"
    assert member["provider"] == "antigravity"
    assert member["reasoning_effort"] == "high"
    assert member["workspace_mode"] == "branch"
    assert member["workspace_path"] == str(tmp_path / "work" / "coder-1")
    assert member["branch_name"] == "feature/coder-1"
    assert member["session_id"] == "sid_coder_1"
    assert member["state"] == "waiting"

    # Single member fetch
    fetched = get_member(conn, "swarm_m", "coder-1")
    assert fetched is not None
    assert fetched["name"] == "coder-1"
    assert fetched["state"] == "waiting"
    assert fetched["model"] == "gemini-2.5-pro"

    # Nonexistent member fetch
    assert get_member(conn, "swarm_m", "unknown") is None

    # Add second member
    add_member(
        conn,
        swarm_id="swarm_m",
        name="reviewer-1",
        role="reviewer",
        initial_state="working",
    )

    # Get all members
    members = get_members(conn, "swarm_m")
    assert len(members) == 2
    names = [m["name"] for m in members]
    assert "coder-1" in names
    assert "reviewer-1" in names

    conn.close()


def test_add_member_validation(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "member_validation.db")
    create_swarm(conn, "s1", "Title", "coord", str(tmp_path))

    # Nonexistent swarm
    with pytest.raises(ValueError, match="Swarm 'nonexistent' does not exist"):
        add_member(conn, "nonexistent", "worker-1")

    # Empty name
    with pytest.raises(ValueError, match="name cannot be empty"):
        add_member(conn, "s1", "")

    # Duplicate member in same swarm
    add_member(conn, "s1", "worker-1")
    with pytest.raises(ValueError, match="Member 'worker-1' already exists"):
        add_member(conn, "s1", "worker-1")

    # Invalid workspace_mode
    with pytest.raises(ValueError, match="Invalid workspace_mode 'unsupported'"):
        add_member(conn, "s1", "worker-2", workspace_mode="unsupported")

    # Invalid initial_state: killed
    with pytest.raises(ValueError, match="Invalid initial state 'killed'"):
        add_member(conn, "s1", "worker-killed", initial_state="killed")

    # Invalid initial_state: bogus
    with pytest.raises(ValueError, match="Invalid initial state 'bogus'"):
        add_member(conn, "s1", "worker-bogus", initial_state="bogus")

    conn.close()


# ==============================================================================
# Worker Lifecycle State Machine
# ==============================================================================


@pytest.mark.parametrize(
    "start_state,next_state",
    [
        ("waiting", "working"),
        ("waiting", "stopped"),
        ("waiting", "killed"),
        ("working", "waiting"),
        ("working", "stopped"),
        ("working", "killed"),
        ("stopped", "waiting"),
        ("stopped", "working"),
        ("stopped", "killed"),
    ],
)
def test_valid_state_transitions(
    tmp_path: Path, start_state: str, next_state: str
) -> None:
    conn = init_swarm_db(tmp_path / f"trans_{start_state}_{next_state}.db")
    create_swarm(conn, "s1", "Title", "coord", str(tmp_path))

    # Add member with start_state
    add_member(conn, "s1", "worker-1", initial_state=start_state)
    mem = get_member(conn, "s1", "worker-1")
    assert mem is not None
    assert mem["state"] == start_state

    # Execute valid transition
    update_member_state(conn, "s1", "worker-1", next_state)
    updated = get_member(conn, "s1", "worker-1")
    assert updated is not None
    assert updated["state"] == next_state

    conn.close()


@pytest.mark.parametrize("state", ["waiting", "working", "stopped"])
def test_idempotent_self_transitions(tmp_path: Path, state: str) -> None:
    conn = init_swarm_db(tmp_path / f"idempotent_{state}.db")
    create_swarm(conn, "s1", "Title", "coord", str(tmp_path))
    add_member(conn, "s1", "worker-1", initial_state=state)

    # Calling with current state is a safe no-op
    update_member_state(conn, "s1", "worker-1", state)
    mem = get_member(conn, "s1", "worker-1")
    assert mem is not None
    assert mem["state"] == state

    conn.close()


@pytest.mark.parametrize("attempted_target", ["working", "waiting", "stopped", "killed"])
def test_killed_is_strictly_terminal(tmp_path: Path, attempted_target: str) -> None:
    conn = init_swarm_db(tmp_path / f"killed_to_{attempted_target}.db")
    create_swarm(conn, "s1", "Title", "coord", str(tmp_path))
    add_member(conn, "s1", "worker-dead", initial_state="working")

    # Transition to killed
    update_member_state(conn, "s1", "worker-dead", "killed")
    mem = get_member(conn, "s1", "worker-dead")
    assert mem is not None
    assert mem["state"] == "killed"

    # Any transition from killed must raise ValueError
    with pytest.raises(
        ValueError,
        match=f"Worker 'worker-dead' is in terminal state 'killed' and cannot transition to '{attempted_target}'",
    ):
        update_member_state(conn, "s1", "worker-dead", attempted_target)

    # Verify state remains killed
    mem_after = get_member(conn, "s1", "worker-dead")
    assert mem_after is not None
    assert mem_after["state"] == "killed"

    conn.close()


def test_invalid_state_and_missing_member_errors(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "state_errors.db")
    create_swarm(conn, "s1", "Title", "coord", str(tmp_path))
    add_member(conn, "s1", "worker-1", initial_state="waiting")

    # Unknown new_state name
    with pytest.raises(ValueError, match="Invalid member state 'bogus_state'"):
        update_member_state(conn, "s1", "worker-1", "bogus_state")

    # Nonexistent member
    with pytest.raises(ValueError, match="Member 'ghost' not found in swarm 's1'"):
        update_member_state(conn, "s1", "ghost", "working")

    conn.close()


# ==============================================================================
# Monotonic Message Appending & Querying
# ==============================================================================


def test_append_message_monotonic_sequential(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "msg_seq.db")
    create_swarm(conn, "s1", "Title 1", "coord", str(tmp_path))

    seq1 = append_message(conn, "s1", "coord", ["@w1"], "Hello w1", hop_count=0)
    assert seq1 == 1

    seq2 = append_message(conn, "s1", "w1", ["@coord"], "Acknowledged", hop_count=1)
    assert seq2 == 2

    seq3 = append_message(conn, "s1", "w2", [], "Idle check", hop_count=2)
    assert seq3 == 3

    # Check query
    messages = get_messages(conn, "s1", since_seq=0, limit=50)
    assert len(messages) == 3
    assert [m["seq"] for m in messages] == [1, 2, 3]
    assert messages[0]["author"] == "coord"
    assert messages[0]["mentions"] == ["@w1"]
    assert messages[0]["hop_count"] == 0

    assert messages[1]["author"] == "w1"
    assert messages[1]["mentions"] == ["@coord"]
    assert messages[1]["hop_count"] == 1

    assert messages[2]["author"] == "w2"
    assert messages[2]["mentions"] == []
    assert messages[2]["hop_count"] == 2

    conn.close()


def test_append_message_independent_swarms(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "multi_swarms_msg.db")
    create_swarm(conn, "swarm_a", "Swarm A", "coord", str(tmp_path))
    create_swarm(conn, "swarm_b", "Swarm B", "coord", str(tmp_path))

    seq_a1 = append_message(conn, "swarm_a", "alice", [], "msg a1")
    seq_b1 = append_message(conn, "swarm_b", "bob", [], "msg b1")
    seq_a2 = append_message(conn, "swarm_a", "alice", [], "msg a2")
    seq_b2 = append_message(conn, "swarm_b", "bob", [], "msg b2")

    assert seq_a1 == 1
    assert seq_a2 == 2
    assert seq_b1 == 1
    assert seq_b2 == 2

    msgs_a = get_messages(conn, "swarm_a")
    msgs_b = get_messages(conn, "swarm_b")
    assert [m["seq"] for m in msgs_a] == [1, 2]
    assert [m["seq"] for m in msgs_b] == [1, 2]

    conn.close()


def test_append_message_validation(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "msg_val.db")
    create_swarm(conn, "s1", "Title", "coord", str(tmp_path))

    # Empty swarm_id
    with pytest.raises(ValueError, match="swarm_id cannot be empty"):
        append_message(conn, "", "alice", [], "test")

    # Empty author
    with pytest.raises(ValueError, match="author cannot be empty"):
        append_message(conn, "s1", "", [], "test")

    # Negative hop_count
    with pytest.raises(ValueError, match="hop_count cannot be negative"):
        append_message(conn, "s1", "alice", [], "test", hop_count=-1)

    # Nonexistent swarm
    with pytest.raises(ValueError, match="Swarm 'nonexistent' does not exist"):
        append_message(conn, "nonexistent", "alice", [], "test")

    conn.close()


def test_get_messages_pagination_window(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "msg_pag.db")
    create_swarm(conn, "s1", "Title", "coord", str(tmp_path))

    # Insert 25 messages
    for i in range(1, 26):
        append_message(conn, "s1", f"user-{i}", ["@all"], f"Message {i}")

    # Page 1: since_seq=0, limit=10 -> seq 1..10
    page1 = get_messages(conn, "s1", since_seq=0, limit=10)
    assert len(page1) == 10
    assert [m["seq"] for m in page1] == list(range(1, 11))

    # Page 2: since_seq=10, limit=10 -> seq 11..20
    page2 = get_messages(conn, "s1", since_seq=10, limit=10)
    assert len(page2) == 10
    assert [m["seq"] for m in page2] == list(range(11, 21))

    # Page 3: since_seq=20, limit=10 -> seq 21..25
    page3 = get_messages(conn, "s1", since_seq=20, limit=10)
    assert len(page3) == 5
    assert [m["seq"] for m in page3] == list(range(21, 26))

    # Page 4: since_seq=25, limit=10 -> empty
    page4 = get_messages(conn, "s1", since_seq=25, limit=10)
    assert page4 == []

    # Empty swarm query
    empty = get_messages(conn, "unknown_swarm")
    assert empty == []

    conn.close()


# ==============================================================================
# Transactions & Savepoints
# ==============================================================================


def test_write_txn_commit_and_rollback(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "tx_test.db")
    create_swarm(conn, "s1", "Title", "coord", str(tmp_path))

    # Rollback on exception
    try:
        with write_txn(conn):
            conn.execute(
                "INSERT INTO swarms (swarm_id, title, coordinator_sid, workspace_root, config, created_at, updated_at) "
                "VALUES ('failed_swarm', 'Failed', 'sid', 'root', '{}', 'now', 'now')"
            )
            raise RuntimeError("Forced abort")
    except RuntimeError:
        pass

    assert get_swarm(conn, "failed_swarm") is None

    # Clean commit
    with write_txn(conn):
        conn.execute(
            "INSERT INTO swarms (swarm_id, title, coordinator_sid, workspace_root, config, created_at, updated_at) "
            "VALUES ('committed_swarm', 'Committed', 'sid', 'root', '{}', 'now', 'now')"
        )

    assert get_swarm(conn, "committed_swarm") is not None
    conn.close()


def test_write_txn_nested_savepoints(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "savepoint.db")
    create_swarm(conn, "s1", "Title", "coord", str(tmp_path))

    with write_txn(conn):
        add_member(conn, "s1", "worker-outer")
        try:
            with write_txn(conn):  # Nested savepoint
                add_member(conn, "s1", "worker-inner-fail")
                raise ValueError("Inner savepoint failure")
        except ValueError:
            pass  # Inner failure caught; outer transaction remains valid

        add_member(conn, "s1", "worker-outer-2")

    members = get_members(conn, "s1")
    member_names = [m["name"] for m in members]
    assert "worker-outer" in member_names
    assert "worker-outer-2" in member_names
    assert "worker-inner-fail" not in member_names

    conn.close()


def test_foreign_key_cascade_delete(tmp_path: Path) -> None:
    conn = init_swarm_db(tmp_path / "cascade.db")
    create_swarm(conn, "s1", "Title", "coord", str(tmp_path))
    add_member(conn, "s1", "w1")
    append_message(conn, "s1", "w1", ["@all"], "msg 1")

    assert get_member(conn, "s1", "w1") is not None
    assert len(get_messages(conn, "s1")) == 1

    # Delete swarm
    with write_txn(conn):
        conn.execute("DELETE FROM swarms WHERE swarm_id = 's1'")

    # Members and messages should be cascade-deleted
    assert get_member(conn, "s1", "w1") is None
    assert get_members(conn, "s1") == []
    assert get_messages(conn, "s1") == []

    conn.close()


# ==============================================================================
# Concurrency Stress Tests
# ==============================================================================


def test_concurrent_message_appends_multithreaded(tmp_path: Path) -> None:
    """Stress test: 8 threads x 25 messages = 200 total messages to a single swarm.

    Must produce strictly monotonic 1..200 sequences with zero gaps, zero duplicates,
    and zero database locked errors.
    """
    db_file = tmp_path / "concurrent_stress.db"
    setup_conn = init_swarm_db(db_file)
    create_swarm(setup_conn, "stress_swarm", "Stress Test", "coord", str(tmp_path))
    setup_conn.close()

    num_threads = 8
    msgs_per_thread = 25
    expected_total = num_threads * msgs_per_thread
    errors: list[Exception] = []

    def worker_thread(thread_idx: int) -> None:
        thread_conn = init_swarm_db(db_file)
        try:
            for i in range(msgs_per_thread):
                append_message(
                    thread_conn,
                    "stress_swarm",
                    f"worker-{thread_idx}",
                    [f"@peer-{i}"],
                    f"Thread {thread_idx} message {i}",
                )
        except Exception as exc:
            errors.append(exc)
        finally:
            thread_conn.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker_thread, t) for t in range(num_threads)]
        concurrent.futures.wait(futures)

    assert len(errors) == 0, f"Encountered concurrency errors: {errors}"

    verify_conn = init_swarm_db(db_file)
    messages = get_messages(verify_conn, "stress_swarm", since_seq=0, limit=expected_total + 10)
    verify_conn.close()

    assert len(messages) == expected_total
    seqs = [m["seq"] for m in messages]
    expected_seqs = list(range(1, expected_total + 1))
    assert seqs == expected_seqs


def test_concurrent_message_appends_multi_swarm(tmp_path: Path) -> None:
    """Stress test: concurrent writers across multiple distinct swarms."""
    db_file = tmp_path / "multi_swarm_stress.db"
    setup_conn = init_swarm_db(db_file)
    create_swarm(setup_conn, "swarm_one", "Swarm One", "coord1", str(tmp_path))
    create_swarm(setup_conn, "swarm_two", "Swarm Two", "coord2", str(tmp_path))
    setup_conn.close()

    errors: list[Exception] = []

    def worker(swarm_id: str, thread_id: int) -> None:
        conn = init_swarm_db(db_file)
        try:
            for i in range(25):
                append_message(conn, swarm_id, f"author_{thread_id}", [], f"m {i}")
        except Exception as exc:
            errors.append(exc)
        finally:
            conn.close()

    threads: list[threading.Thread] = []
    # 4 threads to swarm_one (100 msgs), 4 threads to swarm_two (100 msgs)
    for t in range(4):
        threads.append(threading.Thread(target=worker, args=("swarm_one", t)))
        threads.append(threading.Thread(target=worker, args=("swarm_two", t + 4)))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0

    verify_conn = init_swarm_db(db_file)
    msgs_1 = get_messages(verify_conn, "swarm_one", limit=200)
    msgs_2 = get_messages(verify_conn, "swarm_two", limit=200)
    verify_conn.close()

    assert len(msgs_1) == 100
    assert len(msgs_2) == 100
    assert [m["seq"] for m in msgs_1] == list(range(1, 101))
    assert [m["seq"] for m in msgs_2] == list(range(1, 101))


def test_concurrent_member_state_updates(tmp_path: Path) -> None:
    """Multiple threads toggling member states concurrently."""
    db_file = tmp_path / "member_concurrency.db"
    setup_conn = init_swarm_db(db_file)
    create_swarm(setup_conn, "s1", "Title", "coord", str(tmp_path))
    for i in range(4):
        add_member(setup_conn, "s1", f"worker-{i}", initial_state="waiting")
    setup_conn.close()

    errors: list[Exception] = []

    def worker_updater(member_idx: int) -> None:
        conn = init_swarm_db(db_file)
        name = f"worker-{member_idx}"
        try:
            for _ in range(20):
                update_member_state(conn, "s1", name, "working")
                update_member_state(conn, "s1", name, "waiting")
        except Exception as exc:
            errors.append(exc)
        finally:
            conn.close()

    threads = [threading.Thread(target=worker_updater, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0

    verify_conn = init_swarm_db(db_file)
    members = get_members(verify_conn, "s1")
    verify_conn.close()

    assert len(members) == 4
    for m in members:
        assert m["state"] == "waiting"
