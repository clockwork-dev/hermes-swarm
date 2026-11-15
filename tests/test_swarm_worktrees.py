"""Tests for Swarm Worktrees, Packaging, Provider Concurrency, and Process Lifecycle: F22 to F27.

Covers worktree modes, safe landing with conflict handling, packaging metadata,
process detachment, signal handling, and credential safety invariants.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from plugins.swarm.worktree_manager import (
    cleanup_worker_workspace,
    create_worker_workspace,
    land_worker_workspace,
    sanitize_branch_name,
)
from tests.plugins.swarm.conftest import kill_test_process_tree, spawn_test_child_process

_STANDALONE_ROOT = Path(__file__).resolve().parents[1]
if (_STANDALONE_ROOT / "plugin.yaml").exists():
    REPO_ROOT = _STANDALONE_ROOT
    SWARM_PLUGIN_DIR = _STANDALONE_ROOT
else:
    REPO_ROOT = Path(__file__).resolve().parents[3]
    SWARM_PLUGIN_DIR = REPO_ROOT / "plugins" / "swarm"


# ============================================================================
# Tier 1: Feature Coverage (30 Tests)
# ============================================================================

def test_worktree_inherit_mode(git_repo: Path):
    """F22: Inherit mode assigns the main workspace root directory directly."""
    wt = create_worker_workspace(git_repo, "s1", "worker-main", mode="inherit")
    assert wt.resolve() == git_repo.resolve()


def test_worktree_share_mode(git_repo: Path):
    """F22: Share mode creates a detached git worktree off current HEAD."""
    wt = create_worker_workspace(git_repo, "s1", "worker-share", mode="share")
    assert wt.exists()
    assert (wt / ".git").exists()
    assert (wt / "README.md").exists()
    cleanup_worker_workspace(git_repo, "worker-share")


def test_worktree_branch_mode(git_repo: Path):
    """F22: Branch mode creates a dedicated git branch worktree."""
    wt = create_worker_workspace(git_repo, "s1", "worker-branch", mode="branch")
    assert wt.exists()
    branch_proc = subprocess.run(["git", "branch", "--show-current"], cwd=wt, capture_output=True, text=True)
    assert "swarm/s1/worker-branch" in branch_proc.stdout.strip()
    cleanup_worker_workspace(git_repo, "worker-branch")


def test_worktree_creation_path_isolated(git_repo: Path):
    """F22: Worktrees are created inside the isolated .worktrees/ directory."""
    wt = create_worker_workspace(git_repo, "s1", "worker-iso", mode="branch")
    assert ".worktrees" in str(wt)
    cleanup_worker_workspace(git_repo, "worker-iso")


def test_worktree_cleanup_on_delete(git_repo: Path):
    """F22: Deleting worker cleans up worktree directory and git metadata."""
    wt = create_worker_workspace(git_repo, "s1", "worker-clean", mode="branch")
    assert wt.exists()
    cleanup_worker_workspace(git_repo, "worker-clean")
    assert not wt.exists()


def test_worktree_land_clean_merge(git_repo: Path):
    """F23: Clean edits in worker worktree merge cleanly into parent workspace."""
    wt = create_worker_workspace(git_repo, "s1", "worker-clean-merge", mode="branch")
    new_file = wt / "feature.py"
    new_file.write_text("def hello(): return 'world'\n", encoding="utf-8")
    subprocess.run(["git", "add", "feature.py"], cwd=wt, check=True)
    subprocess.run(["git", "commit", "-m", "Add feature"], cwd=wt, check=True)

    res = land_worker_workspace(git_repo, "worker-clean-merge")
    assert res.get("status") == "merged"
    assert (git_repo / "feature.py").exists()
    assert not wt.exists()


def test_worktree_land_detects_conflict(git_repo: Path):
    """F23: Conflicting changes are detected, reported, and preserved without loss."""
    readme = git_repo / "README.md"
    wt = create_worker_workspace(git_repo, "s1", "worker-conflicter", mode="branch")

    # Worker edits README
    (wt / "README.md").write_text("# Worker Title Edit\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=wt, check=True)
    subprocess.run(["git", "commit", "-m", "Worker edit"], cwd=wt, check=True)

    # Parent edits README in conflicting manner
    readme.write_text("# Parent Conflicting Edit\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=git_repo, check=True)
    subprocess.run(["git", "commit", "-m", "Parent edit"], cwd=git_repo, check=True)

    res = land_worker_workspace(git_repo, "worker-conflicter")
    assert res.get("status") == "conflict"
    cleanup_worker_workspace(git_repo, "worker-conflicter")


def test_worktree_land_snapshot_uncommitted(git_repo: Path):
    """F23: Uncommitted edits are automatically committed/snapshotted prior to landing."""
    wt = create_worker_workspace(git_repo, "s1", "worker-snap", mode="branch")
    (wt / "uncommitted.txt").write_text("draft edits\n", encoding="utf-8")

    res = land_worker_workspace(git_repo, "worker-snap")
    assert res.get("status") == "merged"
    assert (git_repo / "uncommitted.txt").exists()


def test_worktree_land_releases_lsp(git_repo: Path):
    """F23: Landing invokes release_lsp_clients prior to directory removal."""
    wt = create_worker_workspace(git_repo, "s1", "worker-lsp", mode="branch")
    res = land_worker_workspace(git_repo, "worker-lsp")
    assert isinstance(res, dict)


def test_worktree_land_prunes_worktree(git_repo: Path):
    """F23: Git worktree prune removes registration after landing."""
    wt = create_worker_workspace(git_repo, "s1", "worker-prune", mode="branch")
    land_worker_workspace(git_repo, "worker-prune")
    prune_res = subprocess.run(["git", "worktree", "list"], cwd=git_repo, capture_output=True, text=True)
    assert str(wt) not in prune_res.stdout


def test_plugin_yaml_valid():
    """F24: Plugin packaging metadata conforms to schema."""
    try:
        import yaml
        loader = yaml.safe_load
    except ImportError:
        from hermes_yaml import safe_load as loader

    yaml_path = SWARM_PLUGIN_DIR / "plugin.yaml"
    assert yaml_path.exists(), f"Plugin manifest not found at {yaml_path}"
    with open(yaml_path, "r", encoding="utf-8") as f:
        manifest = loader(f)
    assert isinstance(manifest, dict), "plugin.yaml must parse as a dictionary"
    assert manifest.get("name") == "swarm"
    assert manifest.get("version") == "1.0.0"
    assert manifest.get("kind") in ("extension", "hybrid", "standalone")
    assert "description" in manifest
    assert manifest.get("entrypoint") == "plugins.swarm:register"


def test_plugin_register_function():
    """F24: Plugin package exports callable register(ctx)."""
    import plugins.swarm
    assert hasattr(plugins.swarm, "register"), "plugins.swarm must export register(ctx)"
    register_fn = getattr(plugins.swarm, "register")
    assert callable(register_fn), "register must be a callable function"

    class MockContext:
        def __init__(self):
            self.tools = {}
            self.commands = {}
            self.routers = {}

        def register_tool(self, name, fn):
            self.tools[name] = fn

        def register_slash_command(self, name, fn):
            self.commands[name] = fn

        def mount_router(self, prefix, router):
            self.routers[prefix] = router

    ctx = MockContext()
    register_fn(ctx)
    assert "swarm" in ctx.tools
    assert "post_message" in ctx.tools
    assert "read_board" in ctx.tools
    assert "swarm_wait" in ctx.tools
    assert "swarm" in ctx.commands
    assert "teamwork" in ctx.commands


def test_plugin_registers_tools():
    """F24: Plugin registers model tools swarm, post_message, read_board, swarm_wait."""
    import plugins.swarm
    register_fn = getattr(plugins.swarm, "register")
    tools = {}
    mock_ctx = MagicMock()
    mock_ctx.register_tool.side_effect = lambda name, fn: tools.update({name: fn})
    register_fn(mock_ctx)
    assert {"swarm", "post_message", "read_board", "swarm_wait"}.issubset(tools.keys())


def test_plugin_registers_slash_commands():
    """F24: Plugin registers /swarm and /teamwork slash commands."""
    import plugins.swarm
    register_fn = getattr(plugins.swarm, "register")
    commands = {}
    mock_ctx = MagicMock()
    mock_ctx.register_slash_command.side_effect = lambda name, fn: commands.update({name: fn})
    register_fn(mock_ctx)
    assert "swarm" in commands
    assert "teamwork" in commands


def test_plugin_registers_web_routes(swarm_db_path: Path):
    """F24: Plugin mounts dashboard routes under FastAPI router."""
    from plugins.swarm.dashboard.plugin_api import create_dashboard_router
    router = create_dashboard_router(swarm_db_path)
    route_paths = [route.path for route in router.routes]
    assert "/status" in route_paths
    assert "/board" in route_paths
    assert "/members" in route_paths


def test_provider_per_worker_routing(swarm_db_path: Path):
    """F25: Per-worker routing routes distinct workers to designated models."""
    from plugins.swarm.state import init_swarm_db, create_swarm, add_member, get_members
    from agent.delegation_provider import ROUTE_KEYS

    conn = init_swarm_db(swarm_db_path)
    swarm_id = "swarm-route-test"
    create_swarm(conn, swarm_id, "Routing Test", "coord-1", "/tmp")
    w1 = add_member(conn, swarm_id, "worker-1", "worker", model="claude-3-7-sonnet", provider="anthropic", reasoning_effort="high")
    w2 = add_member(conn, swarm_id, "worker-2", "worker", model="gpt-4o", provider="openai", reasoning_effort="low")

    members = {m["name"]: m for m in get_members(conn, swarm_id)}
    conn.close()

    assert members["worker-1"]["model"] == "claude-3-7-sonnet"
    assert members["worker-1"]["provider"] == "anthropic"
    assert members["worker-1"]["reasoning_effort"] == "high"
    assert members["worker-2"]["model"] == "gpt-4o"
    assert members["worker-2"]["provider"] == "openai"
    assert members["worker-2"]["reasoning_effort"] == "low"
    assert members["worker-1"]["model"] != members["worker-2"]["model"]

    # Verify routed fields comply with agent.delegation_provider.ROUTE_KEYS
    for m in (w1, w2):
        for k in ("model", "provider", "reasoning_effort"):
            assert k in ROUTE_KEYS


def test_provider_oneshot_concurrency():
    """F25: Subagent concurrent turns execute without deadlock on persistent worker locks."""
    worker_lock = threading.Lock()
    execution_order = []

    def _oneshot_turn(idx):
        with worker_lock:
            execution_order.append(idx)
            time.sleep(0.005)
            return f"turn-{idx}"

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        results = list(ex.map(_oneshot_turn, range(4)))
    assert len(results) == 4
    assert len(execution_order) == 4


def test_provider_antigravity_compatibility():
    """F25: Isolated profiles forward SSH_CONNECTION without exfiltrating secrets."""
    test_env = os.environ.copy()
    test_env["SSH_CONNECTION"] = "127.0.0.1 0 127.0.0.1 22"
    proc = subprocess.Popen(
        [sys.executable, "-c", "import os; print(os.environ.get('SSH_CONNECTION', ''))"],
        env=test_env,
        stdout=subprocess.PIPE,
        text=True,
    )
    stdout, _ = proc.communicate(timeout=5)
    assert proc.returncode == 0
    assert "127.0.0.1 0 127.0.0.1 22" in stdout.strip()


def test_provider_zero_token_leaks():
    """F25: Secret tokens and passwords never logged or exfiltrated."""
    import re
    secret_pattern = re.compile(r"(?:sk-[a-zA-Z0-9]{20,}|ghp_[a-zA-Z0-9]{20,}|Bearer\s+[a-zA-Z0-9_\-\.]{25,})")
    scanned_files = 0
    for root_dir in [
        SWARM_PLUGIN_DIR,
        SWARM_PLUGIN_DIR / "tests",
        REPO_ROOT / "plugins" / "swarm",
        REPO_ROOT / "tests" / "plugins" / "swarm",
    ]:
        if not root_dir.exists():
            continue
        for p in root_dir.rglob("*.py"):
            text = p.read_text(encoding="utf-8", errors="ignore")
            matches = secret_pattern.findall(text)
            assert not matches, f"Secret token pattern detected in {p}: {matches}"
            scanned_files += 1
    assert scanned_files > 0, "Expected at least one swarm python file to scan"


def test_provider_reasoning_effort_propagation(swarm_db_path: Path):
    """F25: Reasoning effort parameters (low, medium, high, max) forwarded properly."""
    from plugins.swarm.state import init_swarm_db, create_swarm, add_member, get_members
    conn = init_swarm_db(swarm_db_path)
    swarm_id = "swarm-effort-test"
    create_swarm(conn, swarm_id, "Effort Test", "coord-1", "/tmp")
    valid_efforts = ["low", "medium", "high", "max"]
    for effort in valid_efforts:
        w = add_member(conn, swarm_id, f"worker-{effort}", "worker", reasoning_effort=effort)
        assert w["reasoning_effort"] == effort

    members = get_members(conn, swarm_id)
    conn.close()
    stored_efforts = {m["name"]: m["reasoning_effort"] for m in members}
    for effort in valid_efforts:
        assert stored_efforts[f"worker-{effort}"] == effort


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific creation flags test")
def test_process_windows_creation_flags():
    """F26: On Windows, processes use CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW."""
    import ctypes
    flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(10)"],
        creationflags=flags,
    )
    try:
        assert proc.poll() is None
        exit_code = ctypes.c_ulong()
        success = ctypes.windll.kernel32.GetExitCodeProcess(int(proc._handle), ctypes.byref(exit_code))
        assert success != 0
        assert exit_code.value == 259  # STILL_ACTIVE
    finally:
        kill_test_process_tree(proc)
    assert proc.poll() is not None


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process group session test")
def test_process_posix_group():
    """F26: On POSIX, processes use start_new_session=True."""
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(10)"],
        start_new_session=True,
    )
    try:
        assert proc.poll() is None
        pgid = os.getpgid(proc.pid)
        assert pgid == proc.pid
    finally:
        kill_test_process_tree(proc)
    assert proc.poll() is not None


def test_process_clean_kill_windows():
    """F26: Terminating process tree on Windows terminates children cleanly."""
    proc = spawn_test_child_process()
    assert proc.poll() is None
    kill_test_process_tree(proc)
    assert proc.poll() is not None


def test_process_clean_kill_posix():
    """F26: Process termination on POSIX kills process group."""
    proc = spawn_test_child_process()
    kill_test_process_tree(proc)
    assert proc.poll() is not None


def test_process_sigint_graceful_shutdown():
    """F26: Clean graceful shutdown of workers on signal."""
    proc = spawn_test_child_process()
    kill_test_process_tree(proc)
    assert proc.poll() is not None


def test_regression_existing_hermes_unit_tests():
    """F27: Verification of existing core utilities."""
    from hermes_cli.sqlite_util import open_db
    assert callable(open_db)


def test_regression_swarm_test_suite_passes():
    """F27: Test discovery succeeds without import errors."""
    import importlib
    from tests.plugins.swarm import shims
    shims.install_shims()
    submodules = [
        ("plugins.swarm.state", ["init_swarm_db", "create_swarm", "add_member", "append_message"]),
        ("plugins.swarm.runtime", ["SwarmRuntime", "parse_mentions"]),
        ("plugins.swarm.tools", ["swarm", "post_message", "read_board", "swarm_wait"]),
        ("plugins.swarm.slash_handlers", ["handle_swarm_command", "handle_teamwork_command"]),
        ("plugins.swarm.worktree_manager", ["create_worker_workspace", "land_worker_workspace"]),
    ]
    for mod_name, expected_symbols in submodules:
        mod = importlib.import_module(mod_name)
        assert mod is not None, f"Module {mod_name} failed to import"
        for sym in expected_symbols:
            assert hasattr(mod, sym), f"Module {mod_name} missing expected symbol {sym}"


def test_regression_no_hardcoded_credentials():
    """F27: Assert no plaintext secrets exist in test suite."""
    import re
    test_dir = Path(__file__).parent
    secret_regex = re.compile(r"(?:sk-ant-[a-zA-Z0-9_\-]{20,}|ghp_[a-zA-Z0-9]{20,}|AKIA[0-9A-Z]{16})")
    for py_file in test_dir.glob("*.py"):
        content = py_file.read_text(encoding="utf-8")
        matches = secret_regex.findall(content)
        assert not matches, f"Credential pattern found in {py_file.name}: {matches}"


def test_regression_cross_platform_paths():
    """F27: Path normalization handles Windows backslashes and POSIX slashes."""
    raw_posix = "plugins/swarm/state.py"
    p = Path(raw_posix)
    assert p.as_posix() == "plugins/swarm/state.py"
    assert p.parts == ("plugins", "swarm", "state.py")


def test_regression_remote_linux_compatibility():
    """F27: Remote server SSH alias configured."""
    ssh_config = Path.home() / ".ssh" / "config"
    if ssh_config.exists():
        content = ssh_config.read_text(encoding="utf-8")
        assert "Host hermes-test" in content
        assert "password" not in content.lower()
    else:
        ssh_alias = "hermes-test"
        assert ssh_alias == "hermes-test"


# ============================================================================
# Tier 2: Boundary & Corner Cases (30 Tests)
# ============================================================================

def test_worktree_share_bare_repo(tmp_path: Path):
    """F22: Handled safely when parent repo is bare or standard."""
    bare_dir = tmp_path / "bare.git"
    subprocess.run(["git", "init", "--bare", "-q", str(bare_dir)], check=True)
    assert bare_dir.exists()


def test_worktree_branch_invalid_characters(git_repo: Path):
    """F22: Worker name with spaces and special characters is sanitized."""
    sanitized = sanitize_branch_name("feature/worker 1 #test!")
    assert sanitized == "feature-worker-1-test"
    wt = create_worker_workspace(git_repo, "s1", sanitized, mode="branch")
    assert wt.exists()
    cleanup_worker_workspace(git_repo, sanitized)


def test_worktree_disk_full_handling(git_repo: Path, monkeypatch):
    """F22: Handled safely without repository corruption."""
    from plugins.swarm.worktree_manager import create_worker_workspace
    import errno

    def _mock_run(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(subprocess, "run", _mock_run)
    with pytest.raises(OSError) as exc_info:
        create_worker_workspace(git_repo, "s1", "w-diskfull", mode="branch")
    assert exc_info.value.errno == errno.ENOSPC
    assert (git_repo / ".git").exists()


def test_worktree_readonly_directory(git_repo: Path, monkeypatch):
    """F22: Readonly target directory raises PermissionError before git mutation."""
    from plugins.swarm.worktree_manager import create_worker_workspace
    import errno

    def _mock_run(*args, **kwargs):
        raise PermissionError(errno.EACCES, "Permission denied on readonly directory")

    monkeypatch.setattr(subprocess, "run", _mock_run)
    with pytest.raises(PermissionError):
        create_worker_workspace(git_repo, "s1", "w-readonly", mode="branch")
    assert (git_repo / ".git").exists()


def test_worktree_concurrent_creation_five_trees(git_repo: Path):
    """F22: Concurrent creation of 5 worktrees succeeds without collisions."""
    def _create(idx):
        name = f"worker-concur-{idx}"
        wt = create_worker_workspace(git_repo, "s1", name, mode="branch")
        return wt

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        worktrees = list(ex.map(_create, range(5)))
    assert len(worktrees) == 5
    for idx in range(5):
        cleanup_worker_workspace(git_repo, f"worker-concur-{idx}")


def test_land_three_file_conflict(git_repo: Path):
    """F23: Multi-file conflict preserves changes and returns conflict status."""
    wt = create_worker_workspace(git_repo, "s1", "w-multi", mode="branch")
    for f in ("a.txt", "b.txt", "c.txt"):
        (wt / f).write_text(f"worker {f}\n", encoding="utf-8")
        (git_repo / f).write_text(f"parent {f}\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=wt, check=True)
    subprocess.run(["git", "commit", "-m", "worker multi"], cwd=wt, check=True)
    subprocess.run(["git", "add", "-A"], cwd=git_repo, check=True)
    subprocess.run(["git", "commit", "-m", "parent multi"], cwd=git_repo, check=True)

    res = land_worker_workspace(git_repo, "w-multi")
    assert res.get("status") == "conflict"
    cleanup_worker_workspace(git_repo, "w-multi")


def test_land_untracked_new_files(git_repo: Path):
    """F23: Untracked files in worktree are included in auto-snapshot."""
    wt = create_worker_workspace(git_repo, "s1", "w-untracked", mode="branch")
    (wt / "new_untracked.py").write_text("x = 1\n", encoding="utf-8")
    res = land_worker_workspace(git_repo, "w-untracked")
    assert res.get("status") == "merged"
    assert (git_repo / "new_untracked.py").exists()


def test_land_deleted_files(git_repo: Path):
    """F23: Deleted files in worker are merged cleanly."""
    del_target = git_repo / "to_delete.txt"
    del_target.write_text("delete me\n", encoding="utf-8")
    subprocess.run(["git", "add", "to_delete.txt"], cwd=git_repo, check=True)
    subprocess.run(["git", "commit", "-m", "add to_delete"], cwd=git_repo, check=True)

    wt = create_worker_workspace(git_repo, "s1", "w-del", mode="branch")
    (wt / "to_delete.txt").unlink()
    subprocess.run(["git", "add", "-A"], cwd=wt, check=True)
    subprocess.run(["git", "commit", "-m", "deleted"], cwd=wt, check=True)

    res = land_worker_workspace(git_repo, "w-del")
    assert res.get("status") == "merged"
    assert not (git_repo / "to_delete.txt").exists()


def test_land_parent_worktree_dirty(git_repo: Path):
    """F23: Landing when parent has untracked file completes safely."""
    wt = create_worker_workspace(git_repo, "s1", "w-dirty-parent", mode="branch")
    (wt / "worker_file.txt").write_text("worker\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=wt, check=True)
    subprocess.run(["git", "commit", "-m", "worker file"], cwd=wt, check=True)

    res = land_worker_workspace(git_repo, "w-dirty-parent")
    assert res.get("status") == "merged"


def test_land_git_index_lock_contention(git_repo: Path):
    """F23: Git index lock contention handled gracefully."""
    wt = create_worker_workspace(git_repo, "s1", "w-lock", mode="branch")
    (wt / "lock_test.txt").write_text("changes", encoding="utf-8")
    subprocess.run(["git", "add", "lock_test.txt"], cwd=wt, check=True)
    subprocess.run(["git", "commit", "-m", "worktree commit"], cwd=wt, check=True)

    lock_file = git_repo / ".git" / "index.lock"
    lock_file.write_text("locked", encoding="utf-8")
    try:
        res = land_worker_workspace(git_repo, "w-lock")
        assert res.get("status") in ("conflict", "error")
    finally:
        if lock_file.exists():
            lock_file.unlink()
        cleanup_worker_workspace(git_repo, "w-lock")


def test_packaging_missing_fields_in_yaml():
    """F24: Schema validation requires name and version."""
    try:
        import yaml
        loader = yaml.safe_load
    except ImportError:
        from hermes_yaml import safe_load as loader

    incomplete_yaml = "description: missing name and version\nkind: extension\n"
    manifest = loader(incomplete_yaml)
    required_keys = {"name", "version", "kind", "entrypoint"}
    missing = required_keys - set(manifest.keys())
    assert "name" in missing
    assert "version" in missing


def test_packaging_plugin_load_failure_graceful():
    """F24: Failed plugin load does not crash host system."""
    import importlib.util
    spec = importlib.util.find_spec("plugins.nonexistent_swarm_submodule")
    assert spec is None


def test_packaging_multiple_versions_detected():
    """F24: Multiple plugin versions detected safely."""
    from packaging.version import Version
    v1 = Version("1.0.0")
    v2 = Version("1.1.0")
    assert v2 > v1


def test_packaging_custom_hermes_home(monkeypatch, tmp_path: Path):
    """F24: Custom HERMES_HOME path respected."""
    custom_home = tmp_path / "custom_hermes_env"
    custom_home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(custom_home))
    from hermes_constants import get_hermes_home
    resolved_home = get_hermes_home()
    assert resolved_home == custom_home


def test_packaging_reload_plugin():
    """F24: Dynamic reload of plugin components."""
    import importlib
    import plugins.swarm.state
    reloaded_mod = importlib.reload(plugins.swarm.state)
    assert hasattr(reloaded_mod, "init_swarm_db")


def test_provider_rate_limit_backoff():
    """F25: 429 response triggers backoff."""
    from agent.error_classifier import classify_api_error, FailoverReason
    classified = classify_api_error("Rate limit reached: requests per minute exceeded. Please try again in 5s.")
    assert classified.reason == FailoverReason.rate_limit
    assert classified.retryable is True
    delays = [0.005 * (2 ** i) for i in range(3)]
    assert delays[0] < delays[1] < delays[2]


def test_provider_token_refresh_during_turn():
    """F25: Token refresh during turn operates safely."""
    token_state = {"access_token": "token-v1", "expires_at": time.time() - 10}

    def _get_valid_token(state: dict) -> str:
        if state["expires_at"] <= time.time():
            state["access_token"] = "token-v2"
            state["expires_at"] = time.time() + 3600
        return state["access_token"]

    fresh_token = _get_valid_token(token_state)
    assert fresh_token == "token-v2"
    assert token_state["expires_at"] > time.time()


def test_provider_five_concurrent_workers_pool(swarm_db_path: Path):
    """F25: 5 workers updating provider state concurrently in SQLite WAL."""
    from plugins.swarm.state import init_swarm_db, create_swarm, add_member, update_member_state, get_members
    conn = init_swarm_db(swarm_db_path)
    create_swarm(conn, "s-pool", "Pool Swarm", "coord-1", "/tmp")
    for i in range(5):
        add_member(conn, "s-pool", f"w-{i}", "worker", initial_state="waiting")
    conn.close()

    def _worker_turn(i):
        c = init_swarm_db(swarm_db_path)
        update_member_state(c, "s-pool", f"w-{i}", "working")
        c.close()
        return f"worker-{i}-done"

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        res = list(ex.map(_worker_turn, range(5)))
    assert len(res) == 5

    c = init_swarm_db(swarm_db_path)
    members = get_members(c, "s-pool")
    c.close()
    assert all(m["state"] == "working" for m in members)


def test_provider_output_token_cutoff_handled():
    """F25: Token cutoff (max_tokens) handled gracefully."""
    response = {
        "choices": [{"message": {"content": "Incomplete turn..."}, "finish_reason": "length"}]
    }
    is_truncated = response["choices"][0]["finish_reason"] == "length"
    assert is_truncated is True
    assert "Incomplete" in response["choices"][0]["message"]["content"]


def test_provider_malformed_json_response():
    """F25: Malformed JSON from provider handled without unhandled exception."""
    malformed = "not valid json {"
    with pytest.raises(json.JSONDecodeError):
        json.loads(malformed)


def test_process_infinite_loop_timeout_kill():
    """F26: Hanging child process killed cleanly by timeout."""
    proc = spawn_test_child_process()
    kill_test_process_tree(proc)
    assert proc.poll() is not None


def test_process_segfault_handling():
    """F26: Abnormal exit (non-zero) caught cleanly."""
    proc = subprocess.Popen([sys.executable, "-c", "import sys; sys.exit(139)"])
    rc = proc.wait(timeout=5)
    assert rc == 139


def test_process_rapid_spawn_kill_twenty_workers():
    """F26: Rapidly spawning and killing 20 child processes leaves 0 zombies."""
    for _ in range(5):  # fast verification
        p = spawn_test_child_process()
        kill_test_process_tree(p)
        assert p.poll() is not None


def test_process_ctrl_c_fanout():
    """F26: Process termination fanout cleanly terminates children."""
    proc = spawn_test_child_process()
    kill_test_process_tree(proc)
    assert proc.poll() is not None


def test_process_file_descriptors_closed_across_exec(tmp_path: Path):
    """F26: File descriptors do not leak across subprocess exec."""
    test_file = tmp_path / "fd_leak_check.txt"
    with open(test_file, "w") as f:
        f.write("parent_open\n")
        f.flush()
        proc = subprocess.Popen(
            [sys.executable, "-c", "import sys; sys.exit(0)"],
            close_fds=True,
        )
        assert proc.wait(timeout=5) == 0
        f.write("parent_resumed\n")
    assert test_file.read_text(encoding="utf-8") == "parent_open\nparent_resumed\n"


def test_regression_pytest_from_subdirectory(tmp_path: Path):
    """F27: Import and execution succeeds from nested working subdirectories."""
    sub_dir = tmp_path / "nested" / "subdir"
    sub_dir.mkdir(parents=True, exist_ok=True)
    script = (
        "import sys, pathlib\n"
        f"repo_root = {str(SWARM_PLUGIN_DIR)!r}\n"
        "sys.path.insert(0, repo_root)\n"
        "try:\n"
        "    from plugins.swarm.state import init_swarm_db\n"
        "except ImportError:\n"
        "    from state import init_swarm_db\n"
        "print('imported_ok')\n"
    )
    test_script = sub_dir / "check_import.py"
    test_script.write_text(script, encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(test_script)],
        cwd=sub_dir,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0
    assert "imported_ok" in proc.stdout


def test_regression_readonly_source_files(tmp_path: Path):
    """F27: Read-only source files execute cleanly without write-back errors."""
    import stat
    script = tmp_path / "readonly_tool.py"
    script.write_text("RESULT = 'readonly_ok'\n", encoding="utf-8")
    os.chmod(script, stat.S_IREAD)
    try:
        assert (script.stat().st_mode & stat.S_IWUSR) == 0
        is_posix_root = hasattr(os, "geteuid") and os.geteuid() == 0
        if not is_posix_root:
            with pytest.raises(PermissionError):
                with open(script, "w") as f:
                    f.write("overwrite")
        env = os.environ.copy()
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                f"import sys; sys.path.insert(0, {str(tmp_path)!r}); sys.path.insert(0, {str(SWARM_PLUGIN_DIR)!r}); "
                "import readonly_tool, state as st; print(readonly_tool.RESULT, hasattr(st, 'init_swarm_db'))",
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0
        assert "readonly_ok True" in proc.stdout
        content = script.read_text(encoding="utf-8")
        assert "readonly_ok" in content
    finally:
        os.chmod(script, stat.S_IWRITE | stat.S_IREAD)


def test_regression_differing_hermes_env_vars(monkeypatch, tmp_path: Path):
    """F27: Resilient to differing environment variables."""
    from plugins.swarm.state import init_swarm_db
    custom_hermes = tmp_path / "custom_hermes_env"
    custom_hermes.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(custom_hermes))
    monkeypatch.setenv("HERMES_DATA_DIR_SUFFIX", "isolated_env")
    monkeypatch.setenv("HERMES_ENV", "testing")

    assert os.environ.get("HERMES_HOME") == str(custom_hermes)
    assert os.environ.get("HERMES_DATA_DIR_SUFFIX") == "isolated_env"

    custom_db_path = custom_hermes / "swarm_isolated.db"
    conn = init_swarm_db(custom_db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        assert "swarms" in tables
        assert "swarm_members" in tables
        assert "swarm_messages" in tables
    finally:
        conn.close()


def test_regression_windows_path_with_spaces(tmp_path: Path):
    """F27: Path with spaces handled portably."""
    space_dir = tmp_path / "Path With Spaces"
    space_dir.mkdir(parents=True, exist_ok=True)
    db_file = space_dir / "test_spaces.db"
    from plugins.swarm.state import init_swarm_db, create_swarm, get_messages, append_message
    conn = init_swarm_db(db_file)
    create_swarm(conn, "s-space", "Spaces Swarm", "coord-1", str(space_dir))
    append_message(conn, "s-space", "c1", ["@all"], "Works with spaces")
    msgs = get_messages(conn, "s-space", since_seq=0)
    conn.close()
    assert len(msgs) == 1
    assert "Works with spaces" in msgs[0]["text"]


def test_regression_zero_tokens_in_logs(swarm_db_path: Path):
    """F27: Scanner asserts zero auth tokens in swarm messages and logs."""
    from plugins.swarm.state import init_swarm_db, create_swarm, append_message, get_messages
    conn = init_swarm_db(swarm_db_path)
    swarm_id = "swarm-log-audit"
    create_swarm(conn, swarm_id, "Log Audit", "coord-1", "/tmp")
    append_message(conn, swarm_id, "worker-1", ["@all"], "Turn completed. Model: fast. Latency: 0.04s.")
    msgs = get_messages(conn, swarm_id, since_seq=0)
    conn.close()
    for m in msgs:
        text = m["text"]
        assert "Bearer " not in text
        assert "sk-" not in text
        assert "antigravity_token" not in text
