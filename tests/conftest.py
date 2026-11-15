"""Shared fixtures for the Swarm & Teamwork plugin test suite."""

from __future__ import annotations

import importlib
import importlib.util
import os
import queue
import sqlite3
import subprocess
import sys
import threading
import time
import types
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_THIS_DIR = Path(__file__).resolve().parent
_STANDALONE_ROOT = _THIS_DIR.parent
if (_STANDALONE_ROOT / "plugin.yaml").exists():
    SWARM_PLUGIN_DIR = _STANDALONE_ROOT
else:
    SWARM_PLUGIN_DIR = _THIS_DIR.parents[2] / "plugins" / "swarm"

_HERMES_ROOT: Path | None = None
for _candidate in (
    Path(os.environ.get("HERMES_AGENT_ROOT", "")) if os.environ.get("HERMES_AGENT_ROOT") else None,
    _THIS_DIR.parents[2] if len(_THIS_DIR.parents) > 2 and (_THIS_DIR.parents[2] / "hermes_cli").is_dir() else None,
    Path.home() / "AppData" / "Local" / "hermes" / "hermes-agent",
    Path.home() / ".hermes" / "hermes-agent",
    Path("/home/hermes/.hermes/hermes-agent"),
):
    if _candidate and _candidate.is_dir() and (_candidate / "hermes_cli").is_dir():
        _HERMES_ROOT = _candidate
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break


def _bootstrap_plugin_package() -> None:
    """Ensure ``plugins.swarm`` and ``tests.plugins.swarm`` resolve in both repo layouts."""
    if "plugins" not in sys.modules:
        importlib.import_module("plugins")
    plugins_pkg = sys.modules["plugins"]

    init_py = SWARM_PLUGIN_DIR / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        "plugins.swarm",
        init_py,
        submodule_search_locations=[str(SWARM_PLUGIN_DIR)],
    )
    if spec and spec.loader:
        mod = importlib.util.module_from_spec(spec)
        mod.__package__ = "plugins.swarm"
        mod.__path__ = [str(SWARM_PLUGIN_DIR)]  # type: ignore[attr-defined]
        sys.modules["plugins.swarm"] = mod
        setattr(plugins_pkg, "swarm", mod)
        spec.loader.exec_module(mod)
        for sub in ("state", "runtime", "tools", "slash_handlers", "worktree_manager", "dashboard", "dashboard.plugin_api"):
            full_name = f"plugins.swarm.{sub}"
            sub_mod = importlib.import_module(full_name)
            if "." not in sub:
                setattr(mod, sub, sub_mod)

    tests_paths = [str(_THIS_DIR)]
    if _HERMES_ROOT and (_HERMES_ROOT / "tests").is_dir():
        tests_paths.append(str(_HERMES_ROOT / "tests"))

    if "tests" not in sys.modules:
        t_pkg = types.ModuleType("tests")
        t_pkg.__path__ = tests_paths  # type: ignore[attr-defined]
        t_pkg.__package__ = "tests"
        sys.modules["tests"] = t_pkg
    else:
        existing_paths = list(getattr(sys.modules["tests"], "__path__", []))
        for p in tests_paths:
            if p not in existing_paths:
                existing_paths.append(p)
        sys.modules["tests"].__path__ = existing_paths  # type: ignore[attr-defined]

    for pkg_name in ("tests.plugins", "tests.plugins.swarm"):
        if pkg_name not in sys.modules:
            sub_t_pkg = types.ModuleType(pkg_name)
            sub_t_pkg.__path__ = [str(_THIS_DIR)]  # type: ignore[attr-defined]
            sub_t_pkg.__package__ = pkg_name
            sys.modules[pkg_name] = sub_t_pkg

    sys.modules["tests.plugins.swarm.conftest"] = sys.modules[__name__]
    shims_py = _THIS_DIR / "shims.py"
    if shims_py.exists() and "tests.plugins.swarm.shims" not in sys.modules:
        s_spec = importlib.util.spec_from_file_location("tests.plugins.swarm.shims", shims_py)
        if s_spec and s_spec.loader:
            s_mod = importlib.util.module_from_spec(s_spec)
            sys.modules["tests.plugins.swarm.shims"] = s_mod
            s_spec.loader.exec_module(s_mod)
            s_mod.install_shims()


_bootstrap_plugin_package()

from hermes_cli.commands import COMMAND_REGISTRY, CommandDef

# Ensure COMMAND_REGISTRY contains swarm & teamwork even on unpatched Hermes cores
_existing_cmd_names = {c.name for c in COMMAND_REGISTRY}
if "swarm" not in _existing_cmd_names:
    COMMAND_REGISTRY.append(
        CommandDef(
            "swarm",
            "Manage in-session multi-agent swarm (board, status, workers, worktrees)",
            "session",
            args_hint="[<task>|status|board|add|say|land|kill|stop]",
            surfaces=frozenset({"cli", "gateway", "tui"}),
            busy_policy="dispatch",
        )
    )
if "teamwork" not in _existing_cmd_names:
    COMMAND_REGISTRY.append(
        CommandDef(
            "teamwork",
            "Start autonomous Teamwork Swarm Coordinator mode for a task",
            "session",
            args_hint="<task>",
            surfaces=frozenset({"cli", "gateway", "tui"}),
        )
    )

from tests.fakes.fake_llm_provider import FakeLLMServer, Text, ToolCall, write_hermes_home


# ---------------------------------------------------------------------------
# Speed & Optimization Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _cap_swarm_sleeps(monkeypatch):
    """Cap any time.sleep calls in swarm modules to 0.02s max for speed."""
    real_sleep = time.sleep

    def _fast_sleep(s: float):
        real_sleep(min(float(s), 0.02))

    monkeypatch.setattr(time, "sleep", _fast_sleep)


# ---------------------------------------------------------------------------
# State & Database Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def swarm_db_path(tmp_path: Path) -> Path:
    """Provide a path to an isolated swarm SQLite database."""
    return tmp_path / "swarm.db"


@pytest.fixture
def swarm_db(swarm_db_path: Path) -> Generator[sqlite3.Connection, None, None]:
    """Provide an open WAL-mode SQLite connection, cleanly closed on teardown."""
    from plugins.swarm.state import init_swarm_db
    conn = init_swarm_db(swarm_db_path)
    yield conn
    try:
        conn.close()
    except Exception:
        pass


@pytest.fixture
def mock_completion_queue() -> queue.Queue:
    """Provide an in-memory queue mimicking ProcessRegistry.completion_queue."""
    return queue.Queue()


# ---------------------------------------------------------------------------
# LLM & Environment Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fake_llm():
    """Provide a local FakeLLMServer for testing turns without real tokens."""
    server = FakeLLMServer(default_text="Swarm worker turn completed.")
    server.start()
    yield server
    server.stop()


# ---------------------------------------------------------------------------
# Git Repository Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    """Create a temporary initialized git repository with initial commit on main."""
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q"], cwd=repo_dir, check=True)
    subprocess.run(["git", "config", "user.email", "swarm-test@nous.ai"], cwd=repo_dir, check=True)
    subprocess.run(["git", "config", "user.name", "Swarm Tester"], cwd=repo_dir, check=True)
    (repo_dir / "README.md").write_text("# Swarm Test Repo\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=repo_dir, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "Initial commit"], cwd=repo_dir, check=True)
    return repo_dir


# ---------------------------------------------------------------------------
# Context Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def coordinator_ctx(swarm_db_path: Path, tmp_path: Path) -> dict:
    """Context dictionary for a coordinator session."""
    return {
        "role": "swarm_coordinator",
        "swarm_id": "swarm-test-123",
        "session_id": "sid-coord-456",
        "workspace_root": str(tmp_path),
        "db_path": swarm_db_path,
    }


@pytest.fixture
def worker_ctx(swarm_db_path: Path, tmp_path: Path) -> dict:
    """Context dictionary for a worker session."""
    return {
        "role": "swarm_worker",
        "worker_name": "worker-1",
        "swarm_id": "swarm-test-123",
        "session_id": "sid-worker-789",
        "workspace_root": str(tmp_path / "worktrees" / "worker-1"),
        "db_path": swarm_db_path,
    }


# ---------------------------------------------------------------------------
# FastAPI Dashboard TestClient Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def dashboard_client(swarm_db_path: Path) -> TestClient:
    """Provide a TestClient for testing the dashboard REST endpoints."""
    from plugins.swarm.dashboard.plugin_api import create_dashboard_router
    app = FastAPI()
    router = create_dashboard_router(swarm_db_path)
    app.include_router(router, prefix="/api/plugins/swarm")
    return TestClient(app)


# ---------------------------------------------------------------------------
# Cross-Platform Process Management Helpers
# ---------------------------------------------------------------------------

def spawn_test_child_process() -> subprocess.Popen:
    """Spawn an isolated long-running child process with platform detachment flags."""
    from plugins.swarm.worktree_manager import get_detached_process_kwargs

    cmd = [sys.executable, "-c", "import time; time.sleep(60)"]
    return subprocess.Popen(cmd, **get_detached_process_kwargs())


def kill_test_process_tree(proc: subprocess.Popen) -> None:
    """Cleanly terminate process and any descendant children."""
    from plugins.swarm.worktree_manager import terminate_process_tree

    terminate_process_tree(proc)
