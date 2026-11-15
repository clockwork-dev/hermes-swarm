"""Worktree lifecycle, cross-platform process management, and provider routing helpers for Swarm workers."""

from __future__ import annotations

import os
import re
import shutil
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Optional

_REPO_LOCKS: dict[str, threading.RLock] = {}
_REPO_LOCKS_GUARD = threading.Lock()


def _repo_lock(repo_root: Path) -> threading.RLock:
    key = os.path.normcase(str(repo_root.resolve()))
    with _REPO_LOCKS_GUARD:
        return _REPO_LOCKS.setdefault(key, threading.RLock())


def _release_lsp_clients(wt_dir: Path) -> None:
    """Best-effort hook to release LSP handles if already loaded in the current process."""
    return None


def sanitize_branch_name(name: str) -> str:
    """Sanitize worker name to form a valid git ref component."""
    cleaned = re.sub(r"[^a-zA-Z0-9_-]", "-", name)
    cleaned = re.sub(r"-+", "-", cleaned).strip("-")
    return cleaned or "worker"


def create_worker_workspace(
    repo_root: Path | str,
    swarm_id: str,
    worker_name: str,
    mode: str = "inherit",
) -> Path:
    """Provision workspace for a worker based on mode (inherit, share, branch)."""
    repo_root = Path(repo_root).resolve()
    if mode == "inherit":
        return repo_root
    if mode not in ("share", "branch"):
        raise ValueError(f"Unknown workspace mode: {mode}")

    sanitized = sanitize_branch_name(worker_name)
    wt_dir = repo_root / ".worktrees" / sanitized
    with _repo_lock(repo_root):
        wt_dir.parent.mkdir(parents=True, exist_ok=True)

        if mode == "share":
            subprocess.run(
                ["git", "worktree", "add", "--detach", str(wt_dir), "HEAD"],
                cwd=repo_root,
                capture_output=True,
                text=True,
                check=True,
            )
        elif mode == "branch":
            branch_name = f"swarm/{swarm_id}/{sanitized}"
            subprocess.run(
                ["git", "worktree", "add", "-b", branch_name, str(wt_dir), "HEAD"],
                cwd=repo_root,
                capture_output=True,
                text=True,
                check=True,
            )

    return wt_dir


def land_worker_workspace(
    repo_root: Path | str,
    worker_name: str,
    branch_name: Optional[str] = None,
) -> dict[str, Any]:
    """Merge worker branch changes back into parent workspace and clean up worktree."""
    repo_root = Path(repo_root).resolve()
    sanitized = sanitize_branch_name(worker_name)
    wt_dir = repo_root / ".worktrees" / sanitized

    with _repo_lock(repo_root):
        if not wt_dir.exists():
            return {"worker": worker_name, "status": "clean", "changes": 0}

        # Auto-snapshot uncommitted changes if any
        status_proc = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=wt_dir,
            capture_output=True,
            text=True,
        )
        if status_proc.stdout.strip():
            subprocess.run(["git", "add", "-A"], cwd=wt_dir, capture_output=True)
            subprocess.run(
                ["git", "commit", "-m", f"Auto-snapshot before landing {worker_name}"],
                cwd=wt_dir,
                capture_output=True,
            )

        # Release any language server clients holding handles on this worktree
        _release_lsp_clients(wt_dir)

        # Determine branch to merge
        target_branch = branch_name
        if not target_branch:
            branch_proc = subprocess.run(
                ["git", "branch", "--show-current"],
                cwd=wt_dir,
                capture_output=True,
                text=True,
            )
            target_branch = branch_proc.stdout.strip()

        if target_branch:
            merge_proc = subprocess.run(
                ["git", "merge", target_branch],
                cwd=repo_root,
                capture_output=True,
                text=True,
            )
            if merge_proc.returncode != 0:
                subprocess.run(["git", "merge", "--abort"], cwd=repo_root, capture_output=True)
                return {
                    "worker": worker_name,
                    "status": "conflict",
                    "conflicts": [merge_proc.stderr],
                }

        # Cleanup worktree on clean merge
        subprocess.run(
            ["git", "worktree", "remove", "--force", str(wt_dir)],
            cwd=repo_root,
            capture_output=True,
        )
        subprocess.run(["git", "worktree", "prune"], cwd=repo_root, capture_output=True)
        if wt_dir.exists():
            shutil.rmtree(wt_dir, ignore_errors=True)

        return {"worker": worker_name, "status": "merged", "branch": target_branch}


def cleanup_worker_workspace(repo_root: Path | str, worker_name: str) -> None:
    """Force remove worker worktree and prune git worktree metadata."""
    repo_root = Path(repo_root).resolve()
    sanitized = sanitize_branch_name(worker_name)
    wt_dir = repo_root / ".worktrees" / sanitized
    with _repo_lock(repo_root):
        if wt_dir.exists():
            _release_lsp_clients(wt_dir)
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(wt_dir)],
                cwd=repo_root,
                capture_output=True,
            )
            subprocess.run(["git", "worktree", "prune"], cwd=repo_root, capture_output=True)
            shutil.rmtree(wt_dir, ignore_errors=True)


def get_detached_process_kwargs() -> dict[str, Any]:
    """Return cross-platform subprocess.Popen kwargs for detached process group creation."""
    if sys.platform == "win32":
        return {
            "creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
        }
    return {"start_new_session": True}


def terminate_process_tree(proc: subprocess.Popen, timeout: float = 5.0) -> None:
    """Cleanly terminate a subprocess and its entire process tree across Windows and POSIX."""
    if proc.poll() is not None:
        return
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            capture_output=True,
        )
    else:
        try:
            pgid = os.getpgid(proc.pid)
            os.killpg(pgid, signal.SIGTERM)
            try:
                proc.wait(timeout=min(timeout, 1.0))
                return
            except subprocess.TimeoutExpired:
                os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        proc.wait(timeout=timeout)
    except Exception:
        pass


def build_worker_route(
    model: str = "",
    provider: str = "",
    reasoning_effort: Optional[str] = None,
    profile: Optional[str] = None,
) -> dict[str, Any]:
    """Build a per-worker delegation route dictionary conforming to agent.delegation_provider.ROUTE_KEYS."""
    from agent.delegation_provider import ROUTE_KEYS

    raw: dict[str, Any] = {}
    if model:
        raw["model"] = model
    if provider:
        raw["provider"] = provider
    if reasoning_effort:
        raw["reasoning_effort"] = reasoning_effort
    if profile:
        raw["profile"] = profile
    return {k: v for k, v in raw.items() if k in ROUTE_KEYS}
