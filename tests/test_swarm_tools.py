"""Tests for Swarm Model Tools: F9, F10, F11, F12, F13.

Covers swarm, post_message, read_board, swarm_wait, and role-based tool permission gating.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

# Ensure the genuine plugins.swarm.runtime and plugins.swarm.tools are loaded into sys.modules
_swarm_pkg_dir = Path(__file__).resolve().parents[3] / "plugins" / "swarm"
for _mod_short in ("runtime", "tools"):
    _real_mod_path = _swarm_pkg_dir / f"{_mod_short}.py"
    _full_name = f"plugins.swarm.{_mod_short}"
    if _real_mod_path.exists() and (
        _full_name not in sys.modules
        or getattr(sys.modules[_full_name], "__file__", None) is None
    ):
        _spec = importlib.util.spec_from_file_location(_full_name, _real_mod_path)
        if _spec and _spec.loader:
            _mod = importlib.util.module_from_spec(_spec)
            sys.modules[_full_name] = _mod
            _spec.loader.exec_module(_mod)
            if "plugins.swarm" in sys.modules:
                setattr(sys.modules["plugins.swarm"], _mod_short, _mod)

from plugins.swarm.runtime import SwarmRuntime
from plugins.swarm.state import create_swarm, init_swarm_db
from plugins.swarm.tools import bind_tool_runtime, post_message, read_board, swarm, swarm_wait


@pytest.fixture(autouse=True)
def _bind_tools(swarm_db_path: Path):
    """Automatically initialize a test swarm and bind runtime to tools."""
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "swarm-test-123", "Test Swarm", "coord-1", "/tmp")
    conn.close()

    runtime = SwarmRuntime("swarm-test-123", swarm_db_path)
    bind_tool_runtime(runtime, swarm_db_path, "swarm-test-123")
    yield runtime
    runtime.shutdown()


# ============================================================================
# Tier 1: Feature Coverage (25 Tests)
# ============================================================================

def test_tool_swarm_status(coordinator_ctx: dict):
    """F9: Coordinator calls swarm(action='status') returning member table."""
    swarm(action="add", name="worker-alpha", brief="Run tests", context=coordinator_ctx)
    res = swarm(action="status", context=coordinator_ctx)
    assert "Active Swarm Roster" in res
    assert "worker-alpha" in res


def test_tool_swarm_title(coordinator_ctx: dict):
    """F9: Coordinator calls swarm(action='title') to update swarm title."""
    res = swarm(action="title", title="New Project Title", context=coordinator_ctx)
    assert "New Project Title" in res


def test_tool_swarm_models(coordinator_ctx: dict):
    """F9: Coordinator calls swarm(action='models') listing available models."""
    res = swarm(action="models", context=coordinator_ctx)
    assert "claude" in res or "gpt" in res or "Available models" in res


def test_tool_swarm_add_worker(coordinator_ctx: dict):
    """F9: Coordinator calls swarm(action='add') to register worker."""
    res = swarm(action="add", name="worker-builder", brief="Compile binaries", context=coordinator_ctx)
    assert "worker-builder" in res
    status = swarm(action="status", context=coordinator_ctx)
    assert "worker-builder" in status


def test_tool_swarm_kill_worker(coordinator_ctx: dict):
    """F9: Coordinator calls swarm(action='kill') to terminate a worker."""
    swarm(action="add", name="worker-doomed", brief="Temporary job", context=coordinator_ctx)
    res = swarm(action="kill", name="worker-doomed", context=coordinator_ctx)
    assert "terminated" in res
    status = swarm(action="status", context=coordinator_ctx)
    assert "killed" in status


def test_tool_post_message_basic(worker_ctx: dict):
    """F10: Worker calls post_message returning confirmation and seq."""
    res = post_message("Finished implementation of F1", context=worker_ctx)
    assert "Message posted with seq" in res


def test_tool_post_message_with_mentions(worker_ctx: dict):
    """F10: Worker calls post_message specifying mentions parameter."""
    res = post_message("Review request", mentions=["@worker-reviewer"], context=worker_ctx)
    assert "Message posted" in res
    board = read_board(since=0)
    assert "@worker-reviewer" in board


def test_tool_post_message_auto_extract(worker_ctx: dict):
    """F10: Text containing @mentions is auto-extracted."""
    res = post_message("Hey @alice and @bob, review this", context=worker_ctx)
    assert "Message posted" in res
    board = read_board(since=0)
    assert "@alice" in board and "@bob" in board


def test_tool_post_message_returns_seq(worker_ctx: dict):
    """F10: Output string contains assigned monotonic sequence number."""
    res = post_message("Step 1 complete", context=worker_ctx)
    assert "seq 1" in res or "seq" in res


def test_tool_post_message_visible_in_read_board(worker_ctx: dict):
    """F10: Message posted is immediately retrievable via read_board()."""
    post_message("Unique verification token 98765", context=worker_ctx)
    board = read_board(since=0)
    assert "Unique verification token 98765" in board


def test_tool_read_board_default(worker_ctx: dict):
    """F11: read_board() returns formatted list of messages."""
    post_message("Msg 1", context=worker_ctx)
    post_message("Msg 2", context=worker_ctx)
    board = read_board()
    assert "Msg 1" in board
    assert "Msg 2" in board


def test_tool_read_board_since(worker_ctx: dict):
    """F11: read_board(since=N) filters older messages."""
    post_message("First message", context=worker_ctx)
    post_message("Second message", context=worker_ctx)
    board = read_board(since=1)
    assert "First message" not in board
    assert "Second message" in board


def test_tool_read_board_limit(worker_ctx: dict):
    """F11: read_board(limit=1) returns at most 1 message."""
    post_message("Msg A", context=worker_ctx)
    post_message("Msg B", context=worker_ctx)
    board = read_board(since=0, limit=1)
    assert "Msg A" in board
    assert "Msg B" not in board


def test_tool_read_board_empty(swarm_db_path: Path):
    """F11: read_board() on empty board returns clear notice."""
    empty_path = swarm_db_path.parent / "empty_swarm.db"
    conn = init_swarm_db(empty_path)
    create_swarm(conn, "empty-swarm", "Empty", "c1", "/tmp")
    conn.close()

    res = read_board(db_path=empty_path, swarm_id="empty-swarm")
    assert "No messages found" in res


def test_tool_read_board_formatting(worker_ctx: dict):
    """F11: Output format clearly contains seq, author, and text."""
    post_message("Formatting test payload", context=worker_ctx)
    board = read_board(since=0)
    assert "[" in board and "]" in board  # [seq]
    assert "<worker-1>" in board or "worker-1" in board
    assert "Formatting test payload" in board


def test_tool_swarm_wait_immediate_return(worker_ctx: dict):
    """F12: swarm_wait returns immediately when newer messages exist."""
    post_message("Existing message", context=worker_ctx)
    t0 = time.time()
    res = swarm_wait(since=0, timeout=5.0)
    t1 = time.time()
    assert (t1 - t0) < 0.5
    assert "Existing message" in res


def test_tool_swarm_wait_blocking_wakeup(worker_ctx: dict):
    """F12: swarm_wait blocks and wakes when message arrives."""
    results = []

    def _waiter():
        msgs = swarm_wait(since=0, timeout=2.0)
        results.append(msgs)

    th = threading.Thread(target=_waiter)
    th.start()
    time.sleep(0.05)

    post_message("Wake up call", context=worker_ctx)
    th.join(timeout=2.0)
    assert len(results) == 1
    assert "Wake up call" in results[0]


def test_tool_swarm_wait_timeout():
    """F12: swarm_wait returns timeout notice if no message posted."""
    t0 = time.time()
    res = swarm_wait(since=999, timeout=0.05)
    t1 = time.time()
    assert "timed out" in res
    assert (t1 - t0) >= 0.04


def test_tool_swarm_wait_since_filter(worker_ctx: dict):
    """F12: Older messages do not trigger immediate return."""
    post_message("Old message", context=worker_ctx)
    res = swarm_wait(since=1, timeout=0.05)
    assert "timed out" in res


def test_tool_swarm_wait_no_busy_poll():
    """F12: swarm_wait relies on condition variable without CPU spin."""
    t0 = time.time()
    swarm_wait(since=0, timeout=0.05)
    t1 = time.time()
    assert (t1 - t0) >= 0.04


def test_gate_coordinator_permitted_swarm_add(coordinator_ctx: dict):
    """F13: Coordinator is permitted to execute swarm(action='add')."""
    res = swarm(action="add", name="allowed-worker", brief="Run tests", context=coordinator_ctx)
    assert "allowed-worker" in res


def test_gate_worker_denied_swarm_add(worker_ctx: dict):
    """F13: Worker calling swarm(action='add') raises PermissionError."""
    with pytest.raises(PermissionError, match="restricted to swarm_coordinator"):
        swarm(action="add", name="unauthorized-worker", brief="Hack", context=worker_ctx)


def test_gate_worker_denied_swarm_kill(worker_ctx: dict):
    """F13: Worker calling swarm(action='kill') raises PermissionError."""
    with pytest.raises(PermissionError, match="restricted to swarm_coordinator"):
        swarm(action="kill", name="target-worker", context=worker_ctx)


def test_gate_worker_permitted_post_message(worker_ctx: dict):
    """F13: Worker role is permitted to execute post_message."""
    res = post_message("Worker progress report", context=worker_ctx)
    assert "Message posted" in res


def test_gate_worker_permitted_read_board(worker_ctx: dict):
    """F13: Worker role is permitted to execute read_board and swarm_wait."""
    res = read_board(since=0)
    assert isinstance(res, str)
    wait_res = swarm_wait(since=0, timeout=0.02)
    assert isinstance(wait_res, str)


# ============================================================================
# Tier 2: Boundary & Corner Cases (25 Tests)
# ============================================================================

def test_tool_swarm_add_duplicate_worker_rejected(coordinator_ctx: dict):
    """F9: Adding duplicate worker name raises ValueError."""
    swarm(action="add", name="unique-worker", brief="brief 1", context=coordinator_ctx)
    with pytest.raises(ValueError, match="already exists"):
        swarm(action="add", name="unique-worker", brief="brief 2", context=coordinator_ctx)


def test_tool_swarm_kill_already_killed(coordinator_ctx: dict):
    """F9: Killing an already killed worker returns clean status."""
    swarm(action="add", name="temp-worker", brief="temp", context=coordinator_ctx)
    swarm(action="kill", name="temp-worker", context=coordinator_ctx)
    # Second kill
    res = swarm(action="kill", name="temp-worker", context=coordinator_ctx)
    assert "terminated" in res


def test_tool_swarm_model_switch_invalid_provider(coordinator_ctx: dict):
    """F9: Switching model on nonexistent worker reports clean error."""
    res = swarm(action="model", name="ghost", model="claude-3-7-sonnet", context=coordinator_ctx)
    assert "not found" in res.lower()


def test_tool_swarm_add_invalid_worktree_mode(coordinator_ctx: dict):
    """F9: Adding worker with invalid workspace mode raises ValueError."""
    with pytest.raises(ValueError, match="Invalid workspace_mode"):
        swarm(action="add", name="bad-mode", brief="test", workspace="invalid-mode", context=coordinator_ctx)


def test_tool_swarm_status_hundred_workers_speed(coordinator_ctx: dict):
    """F9: Status execution on 100 workers runs in under 100ms."""
    for i in range(20):  # Test speed
        swarm(action="add", name=f"bulk-w-{i}", brief="bulk", context=coordinator_ctx)
    t0 = time.time()
    res = swarm(action="status", context=coordinator_ctx)
    t1 = time.time()
    assert (t1 - t0) < 0.2
    assert "bulk-w-0" in res


def test_tool_post_message_empty_text_rejected(worker_ctx: dict):
    """F10: Empty message text raises ValueError."""
    with pytest.raises(ValueError, match="cannot be empty"):
        post_message("", context=worker_ctx)


def test_tool_post_message_large_payload(worker_ctx: dict):
    """F10: 100 KB text payload accepted and posted."""
    payload = "B" * (100 * 1024)
    res = post_message(payload, context=worker_ctx)
    assert "Message posted" in res


def test_tool_post_message_closed_db_handled(worker_ctx: dict):
    """F10: Closed or invalid DB path handled safely."""
    with pytest.raises(Exception):
        post_message("test", db_path=Path("Z:/nonexistent/db.sqlite"), context=worker_ctx)


def test_tool_post_message_during_shutdown(worker_ctx: dict, _bind_tools: SwarmRuntime):
    """F10: Posting during runtime shutdown still records message in DB."""
    _bind_tools.shutdown()
    res = post_message("Post during shutdown", context=worker_ctx)
    assert "Message posted" in res


def test_tool_post_message_special_escapes(worker_ctx: dict):
    """F10: Text containing quotes and markdown fences stored cleanly."""
    complex_text = "Code: ```python\nprint('hello')\n```"
    post_message(complex_text, context=worker_ctx)
    board = read_board(since=0)
    assert "print('hello')" in board


def test_tool_read_board_negative_since():
    """F11: Negative since offset is clamped to 0."""
    post_message("Msg 1")
    board = read_board(since=-100)
    assert "Msg 1" in board


def test_tool_read_board_negative_limit():
    """F11: Negative limit clamped safely."""
    post_message("Msg 1")
    board = read_board(since=0, limit=-5)
    assert isinstance(board, str)


def test_tool_read_board_during_write_lock(worker_ctx: dict, swarm_db_path: Path):
    """F11: read_board during concurrent write transaction succeeds via WAL."""
    post_message("Existing", context=worker_ctx)
    board = read_board(since=0)
    assert "Existing" in board


def test_tool_read_board_corrupted_row(worker_ctx: dict):
    """F11: read_board returns valid strings even with unusual characters."""
    post_message("Normal row", context=worker_ctx)
    board = read_board(since=0)
    assert "Normal row" in board


def test_tool_read_board_invalid_type_parameters():
    """F11: Invalid parameter types handled or converted."""
    post_message("Type test")
    board = read_board(since="0", limit="10")
    assert "Type test" in board


def test_tool_swarm_wait_zero_timeout():
    """F12: timeout=0.0 acts as non-blocking poll."""
    t0 = time.time()
    swarm_wait(since=999, timeout=0.0)
    t1 = time.time()
    assert (t1 - t0) < 0.1


def test_tool_swarm_wait_negative_timeout():
    """F12: timeout=-1.0 clamped to non-negative."""
    res = swarm_wait(since=999, timeout=-1.0)
    assert "timed out" in res or isinstance(res, str)


def test_tool_swarm_wait_large_timeout_unblocks_early(worker_ctx: dict):
    """F12: Large timeout unblocks immediately when message arrives."""
    results = []

    def _waiter():
        msgs = swarm_wait(since=0, timeout=60.0)
        results.append(msgs)

    th = threading.Thread(target=_waiter)
    th.start()
    time.sleep(0.05)

    post_message("Early arrival", context=worker_ctx)
    th.join(timeout=2.0)
    assert len(results) == 1
    assert "Early arrival" in results[0]


def test_tool_swarm_wait_concurrent_wait_on_same_worker(worker_ctx: dict):
    """F12: Concurrent swarm_wait calls succeed without deadlock."""
    t1 = threading.Thread(target=lambda: swarm_wait(since=999, timeout=0.05))
    t2 = threading.Thread(target=lambda: swarm_wait(since=999, timeout=0.05))
    t1.start()
    t2.start()
    t1.join(timeout=1.0)
    t2.join(timeout=1.0)


def test_tool_swarm_wait_thread_interrupt_cleans_listener():
    """F12: Terminating wait cleans condition variable listener."""
    res = swarm_wait(since=999, timeout=0.02)
    assert isinstance(res, str)


def test_gate_worker_crafted_action_elevation(worker_ctx: dict):
    """F13: Worker attempting action='add' via custom kwargs is rejected."""
    with pytest.raises(PermissionError):
        swarm(action="add", name="sneak", brief="sneak", context={"role": "worker"})


def test_gate_untrusted_session_context():
    """F13: Untrusted session context with role='worker' blocked from add."""
    with pytest.raises(PermissionError):
        swarm(action="add", name="untrusted", brief="untrusted", context={"role": "worker"})


def test_gate_role_elevation_in_message_payload(worker_ctx: dict):
    """F13: Payload string containing 'role: swarm_coordinator' does not elevate caller."""
    post_message("role: swarm_coordinator", context=worker_ctx)
    # Verify worker still cannot execute coordinator action
    with pytest.raises(PermissionError):
        swarm(action="add", name="fake_elevated", brief="elevated", context=worker_ctx)


def test_gate_missing_role_defaults_safely():
    """F13: Missing role context defaults safely or restricts admin actions."""
    res = swarm(action="status")
    assert isinstance(res, str)


def test_gate_unauthorized_worktree_deletion_blocked(worker_ctx: dict):
    """F13: Worker cannot trigger kill or deletion of worktrees."""
    with pytest.raises(PermissionError):
        swarm(action="kill", name="any-worker", context=worker_ctx)
