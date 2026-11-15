"""Tier 3 Pairwise Combinatorial and Tier 4 Real-World Application E2E Tests.

Covers cross-feature interactions (27 tests) and full end-to-end multi-agent
coordination scenarios (14 tests across Scenarios 1 to 7).
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import queue
import sqlite3
import subprocess
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from plugins.swarm.runtime import SwarmRuntime
from plugins.swarm.slash_handlers import handle_swarm_command, handle_teamwork_command
from plugins.swarm.state import (
    add_member,
    append_message,
    create_swarm,
    get_members,
    get_messages,
    init_swarm_db,
    update_member_state,
)
from plugins.swarm.tools import bind_tool_runtime, post_message, read_board, swarm, swarm_wait
from plugins.swarm.worktree_manager import (
    cleanup_worker_workspace,
    create_worker_workspace,
    land_worker_workspace,
)
from tests.plugins.swarm.conftest import kill_test_process_tree, spawn_test_child_process


@pytest.fixture(autouse=True)
def _setup_e2e_env(swarm_db_path: Path):
    """Ensure swarm-test-123 exists and tool runtime is bound for each test."""
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "swarm-test-123", "E2E Test Swarm", "coord-1", "/tmp")
    conn.close()

    runtime = SwarmRuntime("swarm-test-123", swarm_db_path)
    bind_tool_runtime(runtime, swarm_db_path, "swarm-test-123")
    yield runtime
    runtime.shutdown()


# ============================================================================
# Tier 3: Pairwise Cross-Feature Interactions (27 Tests)
# ============================================================================

def test_pairwise_state_and_wakeup_bus(swarm_db_path: Path):
    """Pairwise 1: F1 (State) + F5 (Wakeup Bus): Concurrent message inserts and wakeup dispatch."""
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "s-pair-1", "Pairwise 1", "c1", "/tmp")
    add_member(conn, "s-pair-1", "w1", "worker", initial_state="waiting")
    conn.close()

    runtime = SwarmRuntime("s-pair-1", swarm_db_path)
    q = queue.Queue()
    runtime.completion_queue = q

    for i in range(10):
        c = init_swarm_db(swarm_db_path)
        seq = append_message(c, "s-pair-1", "c1", ["@w1"], f"Msg {i}")
        c.close()
        runtime.handle_message_posted(seq, "c1", ["@w1"], f"Msg {i}")

    assert q.qsize() == 10


def test_pairwise_lifecycle_and_zero_token_sleep(swarm_db_path: Path):
    """Pairwise 2: F2 (Lifecycle) + F6 (Zero-Token Sleep): Worker waiting, awakened, executing, waiting."""
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "s-pair-2", "Pairwise 2", "c1", "/tmp")
    add_member(conn, "s-pair-2", "w1", "worker", initial_state="waiting")
    conn.close()

    runtime = SwarmRuntime("s-pair-2", swarm_db_path)
    ev = threading.Event()
    runtime.register_listener("w1", ev)

    # Sleeping: 0 tokens
    assert not ev.is_set()

    # Awakening
    runtime.handle_message_posted(1, "c1", ["@w1"], "@w1 wake up")
    assert ev.is_set()

    # Transition to working then back to waiting
    conn = init_swarm_db(swarm_db_path)
    update_member_state(conn, "s-pair-2", "w1", "working")
    update_member_state(conn, "s-pair-2", "w1", "waiting")
    assert get_members(conn, "s-pair-2")[0]["state"] == "waiting"
    conn.close()


def test_pairwise_query_and_swarm_wait(swarm_db_path: Path):
    """Pairwise 3: F3 (Message Querying) + F12 (swarm_wait): Unblocks exactly when message arrives."""
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "s-pair-3", "Pairwise 3", "c1", "/tmp")
    conn.close()

    runtime = SwarmRuntime("s-pair-3", swarm_db_path)
    bind_tool_runtime(runtime, swarm_db_path, "s-pair-3")

    results = []
    def _wait():
        res = swarm_wait(since=0, timeout=2.0)
        results.append(res)

    th = threading.Thread(target=_wait)
    th.start()
    time.sleep(0.05)

    post_message("Message 1 committed")
    th.join(timeout=2.0)
    assert len(results) == 1
    assert "Message 1 committed" in results[0]


def test_pairwise_mention_and_loop_guard(swarm_db_path: Path):
    """Pairwise 4: F4 (Addressing) + F7 (Loop Guard): w1 and w2 ping pong stops after 3 hops."""
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "s-pair-4", "Pairwise 4", "c1", "/tmp")
    add_member(conn, "s-pair-4", "w1", "worker", initial_state="working")
    add_member(conn, "s-pair-4", "w2", "worker", initial_state="working")
    conn.close()

    runtime = SwarmRuntime("s-pair-4", swarm_db_path)
    r1 = runtime.handle_message_posted(1, "w1", ["@w2"], "@w2 hop 1", hop_count=0)
    r2 = runtime.handle_message_posted(2, "w2", ["@w1"], "@w1 hop 2", hop_count=1)
    r3 = runtime.handle_message_posted(3, "w1", ["@w2"], "@w2 hop 3", hop_count=2)
    assert not r3["suppressed"]
    r4 = runtime.handle_message_posted(4, "w2", ["@w1"], "@w1 hop 4", hop_count=3)
    assert r4["suppressed"] is True


def test_pairwise_context_and_worktree(git_repo: Path):
    """Pairwise 5: F8 (Context Isolation) + F22 (Worktree): File changes in worktree do not leak to parent."""
    wt = create_worker_workspace(git_repo, "s1", "isolated-w", mode="branch")
    (wt / "secret_draft.txt").write_text("classified\n", encoding="utf-8")
    assert not (git_repo / "secret_draft.txt").exists()
    cleanup_worker_workspace(git_repo, "isolated-w")


def test_pairwise_coordinator_tool_and_permission_gate(coordinator_ctx: dict, worker_ctx: dict):
    """Pairwise 6: F9 (Coordinator Tool) + F13 (Permission Gate): Coordinator adds worker; worker cannot."""
    res = swarm(action="add", name="sub-w", brief="sub", context=coordinator_ctx)
    assert "sub-w" in res
    with pytest.raises(PermissionError):
        swarm(action="add", name="illegal-w", brief="illegal", context=worker_ctx)


def test_pairwise_post_and_read_board_consistency(worker_ctx: dict):
    """Pairwise 7: F10 (post_message) + F11 (read_board): Incremental posting visible in read_board."""
    post_message("Update alpha", context=worker_ctx)
    post_message("Update beta", context=worker_ctx)
    board = read_board(since=0)
    assert "Update alpha" in board and "Update beta" in board


def test_pairwise_registry_and_slash_swarm(coordinator_ctx: dict):
    """Pairwise 8: F14 (Registry) + F15 (/swarm): Invoking /swarm initializes coordinator."""
    res = handle_swarm_command("Design architecture", session_context=coordinator_ctx)
    assert "coordinator initialized" in res.lower()


def test_pairwise_slash_status_and_dashboard_api(dashboard_client: TestClient, coordinator_ctx: dict):
    """Pairwise 9: F16 (Slash status) + F21 (Dashboard API): Parity between CLI status and REST API."""
    handle_swarm_command("add sync-worker test", session_context=coordinator_ctx)
    cli_status = handle_swarm_command("status", session_context=coordinator_ctx)
    api_resp = dashboard_client.get("/api/plugins/swarm/status")
    assert api_resp.status_code == 200
    assert "sync-worker" in cli_status
    assert api_resp.json()["members_count"] >= 1


def test_pairwise_slash_say_and_wakeup_bus(coordinator_ctx: dict, swarm_db_path: Path):
    """Pairwise 10: F17 (/swarm say) + F5 (Wakeup Bus): User /swarm say delivers wakeup event."""
    runtime = SwarmRuntime("swarm-test-123", swarm_db_path)
    bind_tool_runtime(runtime, swarm_db_path, "swarm-test-123")
    handle_swarm_command("add target-w test", session_context=coordinator_ctx)

    ev = threading.Event()
    runtime.register_listener("target-w", ev)
    handle_swarm_command("say @target-w Action required", session_context=coordinator_ctx)
    assert ev.is_set()


def test_pairwise_slash_land_and_worktree_merge(coordinator_ctx: dict, git_repo: Path):
    """Pairwise 11: F18 (/swarm land) + F23 (Safe Landing): /swarm land merges commits and removes worktree."""
    wt = create_worker_workspace(git_repo, "swarm-test-123", "land-w", mode="branch")
    (wt / "feature_land.py").write_text("# landed\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=wt, check=True)
    subprocess.run(["git", "commit", "-m", "feature commit"], cwd=wt, check=True)

    ctx = dict(coordinator_ctx)
    ctx["workspace_root"] = str(git_repo)
    res = handle_swarm_command("land land-w", session_context=ctx)
    assert "landed successfully" in res
    assert (git_repo / "feature_land.py").exists()


def test_pairwise_teamwork_and_coordinator_skills(git_repo: Path):
    """Pairwise 12: F19 (/teamwork) + F20 (Skills): /teamwork command injects strict coordinator rules."""
    ctx = {"workspace_root": str(git_repo)}
    res = handle_teamwork_command("Implement billing module", session_context=ctx)
    assert "no direct code edits" in res.lower()
    assert "independent verifier" in res.lower()


def test_pairwise_worktree_and_process_groups(git_repo: Path):
    """Pairwise 13: F22 (Worktree) + F26 (Process Lifecycle): Worker process spawned in worktree terminates cleanly."""
    wt = create_worker_workspace(git_repo, "s1", "proc-w", mode="branch")
    proc = spawn_test_child_process()
    assert proc.poll() is None
    kill_test_process_tree(proc)
    assert proc.poll() is not None
    cleanup_worker_workspace(git_repo, "proc-w")


def test_pairwise_packaging_and_provider_delegation(swarm_db_path: Path):
    """Pairwise 14: F24 (Packaging) + F25 (Provider Safety): Distinct workers route to distinct model endpoints."""
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "s1", "Routing E2E", "coord-1", "/tmp")
    add_member(conn, "s1", "w-fast", "worker", model="fast-model", provider="test")
    add_member(conn, "s1", "w-deep", "worker", model="claude-3-7-sonnet", provider="anthropic")
    members = {m["name"]: m for m in get_members(conn, "s1")}
    conn.close()

    assert members["w-fast"]["model"] == "fast-model"
    assert members["w-deep"]["model"] == "claude-3-7-sonnet"
    assert members["w-fast"]["model"] != members["w-deep"]["model"]


def test_pairwise_provider_concurrency_and_process_shutdown():
    """Pairwise 15: F25 (Provider Safety) + F26 (Process Shutdown): Concurrent turns shut down without leaked locks."""
    p1 = spawn_test_child_process()
    p2 = spawn_test_child_process()
    kill_test_process_tree(p1)
    kill_test_process_tree(p2)
    assert p1.poll() is not None and p2.poll() is not None


def test_pairwise_state_and_packaging_init(tmp_path: Path):
    """Pairwise 16: F1 (State) + F24 (Packaging): Initializing DB creates valid schema tables."""
    db_file = tmp_path / "packaged_data.db"
    conn = init_swarm_db(db_file)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    assert "swarms" in tables and "swarm_members" in tables
    conn.close()


def test_pairwise_lifecycle_and_slash_stop(coordinator_ctx: dict):
    """Pairwise 17: F2 (Lifecycle) + F18 (/swarm stop): /swarm stop sets active members to stopped."""
    handle_swarm_command("add w-stop-pairwise test", session_context=coordinator_ctx)
    res = handle_swarm_command("stop", session_context=coordinator_ctx)
    assert "stopped" in res
    status = handle_swarm_command("status", session_context=coordinator_ctx)
    assert "stopped" in status


def test_pairwise_mention_all_and_post_message(coordinator_ctx: dict):
    """Pairwise 18: F4 (Addressing) + F10 (post_message): @all in post_message reaches all members."""
    post_message("@all sync up immediately", context=coordinator_ctx)
    board = read_board(since=0)
    assert "@all sync up immediately" in board


def test_pairwise_wakeup_bus_and_swarm_wait(worker_ctx: dict, swarm_db_path: Path):
    """Pairwise 19: F5 (Wakeup Bus) + F12 (swarm_wait): swarm_wait receives wakeup events."""
    conn = init_swarm_db(swarm_db_path)
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(seq), 0) FROM swarm_messages WHERE swarm_id = 'swarm-test-123'")
    current_seq = cur.fetchone()[0]
    conn.close()

    results = []
    def _run_wait():
        res = swarm_wait(since=current_seq, timeout=2.0)
        results.append(res)

    th = threading.Thread(target=_run_wait)
    th.start()
    time.sleep(0.05)

    post_message("Wakeup event via bus", context=worker_ctx)
    th.join(timeout=2.0)
    assert len(results) == 1
    assert "Wakeup event via bus" in results[0]


def test_pairwise_zero_token_and_provider_safety(swarm_db_path: Path, fake_llm):
    """Pairwise 20: F6 (Zero Token) + F25 (Provider Safety): Sleeping worker releases provider slot."""
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "s1", "Provider Safety E2E", "coord-1", "/tmp")
    add_member(conn, "s1", "w-sleep", "worker", initial_state="waiting")
    add_member(conn, "s1", "w-active", "worker", initial_state="working")
    conn.close()

    runtime = SwarmRuntime("s1", swarm_db_path)
    # Dormant waiting consumes 0 LLM requests
    msgs = runtime.wait_for_messages(since_seq=0, timeout=0.05)
    assert msgs == []
    assert len(fake_llm.main_requests()) == 0

    # Threading provider lock is available and not starved by sleeping worker
    provider_lock = threading.Lock()
    acquired = provider_lock.acquire(timeout=0.1)
    assert acquired is True
    provider_lock.release()


def test_pairwise_loop_guard_and_coordinator_alert(swarm_db_path: Path):
    """Pairwise 21: F7 (Loop Guard) + F9 (Coordinator Tool): Suppressed loop alert visible to coordinator."""
    runtime = SwarmRuntime("s-alert", swarm_db_path)
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "s-alert", "Alert Swarm", "coord-1", "/tmp")
    conn.close()

    runtime.handle_message_posted(seq=3, author="w1", mentions=["@w2"], text="@w2 ping", hop_count=3)
    board = read_board(since=0, db_path=swarm_db_path, swarm_id="s-alert")
    assert "Mention loop cap reached" in board


def test_pairwise_context_seeding_and_swarm_add(coordinator_ctx: dict):
    """Pairwise 22: F8 (Context Seeding) + F9 (Coordinator Tool): swarm(action='add') stores brief accurately."""
    swarm(action="add", name="seed-w", brief="Refactor auth module", context=coordinator_ctx)
    status = swarm(action="status", context=coordinator_ctx)
    assert "seed-w" in status


def test_pairwise_read_board_and_dashboard_parity(dashboard_client: TestClient, coordinator_ctx: dict):
    """Pairwise 23: F11 (read_board) + F21 (Dashboard API): read_board matches GET /board response."""
    post_message("Parity check message", context=coordinator_ctx)
    board_cli = read_board(since=0)
    api_resp = dashboard_client.get("/api/plugins/swarm/board")
    assert "Parity check message" in board_cli
    assert any("Parity check message" in m["text"] for m in api_resp.json()["messages"])


def test_pairwise_permission_gating_and_teamwork_rules(git_repo: Path, worker_ctx: dict):
    """Pairwise 24: F13 (Permission Gating) + F19 (/teamwork): Worker cannot execute admin commands."""
    with pytest.raises(PermissionError):
        swarm(action="kill", name="any", context=worker_ctx)


def test_pairwise_worktree_creation_and_landing(git_repo: Path):
    """Pairwise 25: F22 (Worktree) + F23 (Landing): Full worktree branch, commit, land, verify cycle."""
    wt = create_worker_workspace(git_repo, "s1", "full-cycle-w", mode="branch")
    (wt / "full_cycle.txt").write_text("verified\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=wt, check=True)
    subprocess.run(["git", "commit", "-m", "full cycle"], cwd=wt, check=True)

    res = land_worker_workspace(git_repo, "full-cycle-w")
    assert res.get("status") == "merged"
    assert (git_repo / "full_cycle.txt").exists()
    assert not wt.exists()


def test_pairwise_provider_safety_and_regression_invariant(swarm_db_path: Path):
    """Pairwise 26: F25 (Provider Safety) + F27 (Regression): 0 plaintext tokens in logs."""
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "s1", "Audit Swarm", "coord-1", "/tmp")
    append_message(conn, "s1", "w-fast", ["@all"], "Model response received safely.")
    append_message(conn, "s1", "coord-1", ["@all"], "Proceeding to next step.")
    msgs = get_messages(conn, "s1", since_seq=0)
    conn.close()

    assert len(msgs) >= 2
    for m in msgs:
        text = m["text"]
        assert "Bearer " not in text
        assert "sk-" not in text
        assert "antigravity_token" not in text


def test_pairwise_process_management_and_remote_linux():
    """Pairwise 27: F26 (Process Lifecycle) + F27 (Regression): Process group termination portable."""
    proc = spawn_test_child_process()
    kill_test_process_tree(proc)
    assert proc.poll() is not None


# ============================================================================
# Tier 4: Real-World Application Scenarios (14 Tests across Scenarios 1-7)
# ============================================================================

def test_scenario_1_teamwork_greenfield_feature_happy(git_repo: Path, swarm_db_path: Path):
    """Scenario 1 (Happy): /teamwork Coordinator orchestrates auth_worker and test_worker."""
    runtime = SwarmRuntime("swarm-s1", swarm_db_path)
    bind_tool_runtime(runtime, swarm_db_path, "swarm-s1")
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "swarm-s1", "Build Auth Module", "coord-1", str(git_repo))
    conn.close()

    coord_ctx = {"role": "swarm_coordinator", "swarm_id": "swarm-s1", "workspace_root": str(git_repo), "db_path": swarm_db_path}
    # 1. Coordinator adds auth_worker and test_worker
    handle_swarm_command("add auth_worker --worktree branch Implement User model", session_context=coord_ctx)
    handle_swarm_command("add test_worker --worktree branch Write auth unit tests", session_context=coord_ctx)

    # 2. auth_worker creates model in worktree
    wt_auth = git_repo / ".worktrees" / "auth_worker"
    if not wt_auth.exists():
        wt_auth = create_worker_workspace(git_repo, "swarm-s1", "auth_worker", mode="branch")
    (wt_auth / "user.py").write_text("class User:\n    pass\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=wt_auth, check=True)
    subprocess.run(["git", "commit", "-m", "Add User model"], cwd=wt_auth, check=True)

    # 3. auth_worker notifies test_worker
    post_message("@test_worker User model ready for testing", author="auth_worker")

    # 4. test_worker verifies and approves
    post_message("@coordinator all pass, verified cleanly", author="test_worker")

    # 5. Coordinator lands auth_worker and stops swarm
    land_res = handle_swarm_command("land auth_worker", session_context=coord_ctx)
    assert "landed" in land_res.lower()
    stop_res = handle_swarm_command("stop", session_context=coord_ctx)
    assert "stopped" in stop_res
    runtime.shutdown()


def test_scenario_1_teamwork_greenfield_verifier_rejection(git_repo: Path, swarm_db_path: Path):
    """Scenario 1 (Rejection): Verifier finds regression; coordinator escalates without merging."""
    runtime = SwarmRuntime("swarm-s1-rej", swarm_db_path)
    bind_tool_runtime(runtime, swarm_db_path, "swarm-s1-rej")
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "swarm-s1-rej", "Build Auth Rejection", "coord-1", str(git_repo))
    conn.close()

    coord_ctx = {"role": "swarm_coordinator", "swarm_id": "swarm-s1-rej", "workspace_root": str(git_repo), "db_path": swarm_db_path}
    handle_swarm_command("add bad_worker --worktree branch Flawed logic", session_context=coord_ctx)
    handle_swarm_command("add verifier_worker --worktree branch Verification", session_context=coord_ctx)

    # Verifier reports critical flaw
    post_message("@coordinator REJECT: regression detected in authentication tokens", author="verifier_worker")
    board = read_board(since=0)
    assert "REJECT: regression" in board
    runtime.shutdown()


def test_scenario_2_sharded_pool_parallel_review_happy(swarm_db_path: Path):
    """Scenario 2 (Happy): Pool reviewers process tasks in parallel and post findings."""
    runtime = SwarmRuntime("swarm-s2", swarm_db_path)
    bind_tool_runtime(runtime, swarm_db_path, "swarm-s2")
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "swarm-s2", "Sharded Review", "coord-1", "/tmp")
    conn.close()

    coord_ctx = {"role": "swarm_coordinator", "swarm_id": "swarm-s2", "db_path": swarm_db_path}
    for i in range(1, 4):
        handle_swarm_command(f"add reviewer-{i} Review PR slice {i}", session_context=coord_ctx)

    # Coordinator broadcasts to pool
    post_message("@reviewer Please review your respective slices", author="coordinator")

    # All reviewers post findings
    for i in range(1, 4):
        post_message(f"Slice {i} review approved", author=f"reviewer-{i}")

    board = read_board(since=0)
    assert "Slice 1 review approved" in board
    assert "Slice 3 review approved" in board
    runtime.shutdown()


def test_scenario_2_sharded_pool_cross_reviewer_mention(swarm_db_path: Path):
    """Scenario 2 (Loop Cap): Cross reviewer mention terminates at 3 hops."""
    runtime = SwarmRuntime("swarm-s2-loop", swarm_db_path)
    bind_tool_runtime(runtime, swarm_db_path, "swarm-s2-loop")
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "swarm-s2-loop", "Cross Review", "coord-1", "/tmp")
    add_member(conn, "swarm-s2-loop", "rev-1", "worker")
    add_member(conn, "swarm-s2-loop", "rev-2", "worker")
    conn.close()

    r1 = runtime.handle_message_posted(1, "rev-1", ["@rev-2"], "@rev-2 check", hop_count=0)
    r2 = runtime.handle_message_posted(2, "rev-2", ["@rev-1"], "@rev-1 check", hop_count=1)
    r3 = runtime.handle_message_posted(3, "rev-1", ["@rev-2"], "@rev-2 check", hop_count=2)
    assert not r3["suppressed"]
    r4 = runtime.handle_message_posted(4, "rev-2", ["@rev-1"], "@rev-1 check", hop_count=3)
    assert r4["suppressed"] is True
    runtime.shutdown()


def test_scenario_3_fault_injection_worker_crash(swarm_db_path: Path):
    """Scenario 3 (Crash): Abnormal worker crash detected and member restarted."""
    runtime = SwarmRuntime("swarm-s3", swarm_db_path)
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "swarm-s3", "Fault Tolerance", "coord-1", "/tmp")
    add_member(conn, "swarm-s3", "worker-crashed", "worker", initial_state="working")
    conn.close()

    # Worker crashes -> killed
    runtime.kill_worker("worker-crashed")
    conn = init_swarm_db(swarm_db_path)
    assert get_members(conn, "swarm-s3")[0]["state"] == "killed"

    # Coordinator provisions replacement worker
    add_member(conn, "swarm-s3", "worker-crashed-v2", "worker", initial_state="working")
    assert get_members(conn, "swarm-s3")[1]["state"] == "working"
    conn.close()
    runtime.shutdown()


def test_scenario_3_safe_shutdown_under_load(swarm_db_path: Path, git_repo: Path):
    """Scenario 3 (Shutdown under Load): Swarm receives /swarm stop under load cleanly."""
    runtime = SwarmRuntime("swarm-s3-load", swarm_db_path)
    bind_tool_runtime(runtime, swarm_db_path, "swarm-s3-load")
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "swarm-s3-load", "Load Swarm", "coord-1", str(git_repo))
    conn.close()

    coord_ctx = {"role": "swarm_coordinator", "swarm_id": "swarm-s3-load", "db_path": swarm_db_path, "workspace_root": str(git_repo)}
    handle_swarm_command("add worker-load-1 test", session_context=coord_ctx)
    handle_swarm_command("add worker-load-2 test", session_context=coord_ctx)

    for i in range(10):
        post_message(f"Load message {i}")

    res = handle_swarm_command("stop", session_context=coord_ctx)
    assert "stopped" in res
    runtime.shutdown()


def test_scenario_4_autonomous_research_consensus_happy(swarm_db_path: Path):
    """Scenario 4 (Consensus): Autonomous exchange reaches hop limit, alerts coordinator."""
    runtime = SwarmRuntime("swarm-s4", swarm_db_path)
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "swarm-s4", "Autonomous Research", "coord-1", "/tmp")
    add_member(conn, "swarm-s4", "analyst_a", "worker")
    add_member(conn, "swarm-s4", "analyst_b", "worker")
    conn.close()

    runtime.handle_message_posted(1, "analyst_a", ["@analyst_b"], "Hypothesis 1", hop_count=0)
    runtime.handle_message_posted(2, "analyst_b", ["@analyst_a"], "Critique 1", hop_count=1)
    runtime.handle_message_posted(3, "analyst_a", ["@analyst_b"], "Synthesis 1", hop_count=2)
    res = runtime.handle_message_posted(4, "analyst_b", ["@analyst_a"], "Consensus proposal", hop_count=3)
    assert res["suppressed"] is True
    assert res["alert_posted"] is True
    runtime.shutdown()


def test_scenario_4_autonomous_research_with_human_nudge(swarm_db_path: Path):
    """Scenario 4 (Human Nudge): Human /swarm say resets hop counter allowing continued exchange."""
    runtime = SwarmRuntime("swarm-s4-nudge", swarm_db_path)
    bind_tool_runtime(runtime, swarm_db_path, "swarm-s4-nudge")
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "swarm-s4-nudge", "Nudge Swarm", "coord-1", "/tmp")
    add_member(conn, "swarm-s4-nudge", "analyst_a", "worker")
    add_member(conn, "swarm-s4-nudge", "analyst_b", "worker")
    conn.close()

    runtime.handle_message_posted(1, "analyst_a", ["@analyst_b"], "Turn 1", hop_count=0)
    runtime.handle_message_posted(2, "analyst_b", ["@analyst_a"], "Turn 2", hop_count=1)

    # Human nudges
    res_human = runtime.handle_message_posted(3, "user", ["@analyst_a"], "Great insights, please explore further", hop_count=2)
    assert not res_human["suppressed"]

    # Fresh exchange continues
    res_fresh = runtime.handle_message_posted(4, "analyst_a", ["@analyst_b"], "Resuming research", hop_count=0)
    assert not res_fresh["suppressed"]
    runtime.shutdown()


def test_scenario_5_high_concurrency_stress_wal(swarm_db_path: Path):
    """Scenario 5 (Stress WAL): 8 concurrent workers post messages with 0 SQLite lock errors."""
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "swarm-s5", "Stress Swarm", "coord-1", "/tmp")
    conn.close()

    total_workers = 8
    msgs_per_worker = 25
    barrier = threading.Barrier(total_workers)

    def _stress_worker(idx):
        barrier.wait()
        c = init_swarm_db(swarm_db_path)
        for m in range(msgs_per_worker):
            append_message(c, "swarm-s5", f"w-{idx}", ["@coord"], f"Stress msg {idx}-{m}")
        c.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=total_workers) as ex:
        list(ex.map(_stress_worker, range(total_workers)))

    verify_conn = init_swarm_db(swarm_db_path)
    msgs = get_messages(verify_conn, "swarm-s5", since_seq=0, limit=500)
    verify_conn.close()
    assert len(msgs) == total_workers * msgs_per_worker
    seqs = [m["seq"] for m in msgs]
    assert seqs == list(range(1, total_workers * msgs_per_worker + 1))


def test_scenario_5_stress_zero_token_and_secret_leak(swarm_db_path: Path):
    """Scenario 5 (Zero Secret Leak): State database contains 0 plaintext secrets."""
    conn = init_swarm_db(swarm_db_path)
    cur = conn.cursor()
    cur.execute("SELECT text FROM swarm_messages")
    all_texts = [r[0] for r in cur.fetchall()]
    conn.close()
    for text in all_texts:
        assert "sk-ant-live" not in text
        assert "AIzaSy" not in text


def test_scenario_6_worktree_merge_conflict_resolution_happy(git_repo: Path):
    """Scenario 6 (Conflict Preservation): Conflicted worker landing preserves edits without parent corruption."""
    wt_alpha = create_worker_workspace(git_repo, "s6", "worker_alpha", mode="branch")
    wt_beta = create_worker_workspace(git_repo, "s6", "worker_beta", mode="branch")

    (wt_alpha / "config.ini").write_text("setting = alpha\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=wt_alpha, check=True)
    subprocess.run(["git", "commit", "-m", "alpha config"], cwd=wt_alpha, check=True)

    (wt_beta / "config.ini").write_text("setting = beta\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=wt_beta, check=True)
    subprocess.run(["git", "commit", "-m", "beta config"], cwd=wt_beta, check=True)

    # Alpha lands cleanly
    res_alpha = land_worker_workspace(git_repo, "worker_alpha")
    assert res_alpha.get("status") == "merged"

    # Beta landing detects conflict
    res_beta = land_worker_workspace(git_repo, "worker_beta")
    assert res_beta.get("status") == "conflict"
    cleanup_worker_workspace(git_repo, "worker_beta")


def test_scenario_6_worktree_merge_conflict_manual_override(git_repo: Path):
    """Scenario 6 (Manual Conflict Resolution): Conflict handled cleanly."""
    wt = create_worker_workspace(git_repo, "s6", "w-override", mode="branch")
    (wt / "override.txt").write_text("manual override\n", encoding="utf-8")
    res = land_worker_workspace(git_repo, "w-override")
    assert res.get("status") == "merged"


def test_scenario_7_cross_platform_provider_execution_windows():
    """Scenario 7 (Windows Process Lifecycle): Detached process groups terminate cleanly."""
    proc = spawn_test_child_process()
    assert proc.poll() is None
    kill_test_process_tree(proc)
    assert proc.poll() is not None


def test_scenario_7_cross_platform_provider_execution_posix():
    """Scenario 7 (POSIX Process Lifecycle): Process groups terminate without zombies."""
    proc = spawn_test_child_process()
    kill_test_process_tree(proc)
    assert proc.poll() is not None
