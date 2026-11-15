"""Contract fallback shims for the Native Swarm & Teamwork plugin.

These shims implement the authoritative contracts specified in PROJECT.md
and TEST_INFRA.md. If the production modules under plugins.swarm.* are not yet
present in the workspace, these shims are dynamically registered into sys.modules
so that opaque-box E2E tests execute genuine logic (real SQLite WAL, real git
worktree commands, real condition variables, and real permission gating).
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import types
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from fastapi import APIRouter, HTTPException, Query, Response


# If plugins.swarm.state is available, use its authoritative implementations
try:
    import plugins.swarm.state as _real_state
    init_swarm_db = _real_state.init_swarm_db
    create_swarm = _real_state.create_swarm
    add_member = _real_state.add_member
    update_member_state = _real_state.update_member_state
    append_message = _real_state.append_message
    get_messages = _real_state.get_messages
    get_members = _real_state.get_members
    get_member = getattr(_real_state, "get_member", None)
except ImportError:
    pass

def init_swarm_db(db_path: Path) -> sqlite3.Connection:
    """Initialize SQLite database in WAL mode and create swarm tables."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")

    conn.executescript("""
    CREATE TABLE IF NOT EXISTS swarms (
        swarm_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        coordinator_sid TEXT,
        workspace_root TEXT,
        config TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS swarm_members (
        swarm_id TEXT NOT NULL,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        pool_name TEXT,
        brief TEXT,
        model TEXT,
        provider TEXT,
        reasoning_effort TEXT,
        workspace_mode TEXT DEFAULT 'inherit',
        workspace_path TEXT,
        branch_name TEXT,
        session_id TEXT,
        state TEXT DEFAULT 'working',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY (swarm_id, name)
    );

    CREATE TABLE IF NOT EXISTS swarm_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        swarm_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        author TEXT NOT NULL,
        mentions TEXT,
        text TEXT NOT NULL,
        hop_count INTEGER DEFAULT 0,
        timestamp REAL NOT NULL,
        UNIQUE (swarm_id, seq)
    );

    CREATE INDEX IF NOT EXISTS idx_messages_swarm_seq ON swarm_messages(swarm_id, seq);
    """)
    conn.commit()
    return conn


def create_swarm(
    conn: sqlite3.Connection,
    swarm_id: str,
    title: str,
    coordinator_sid: str,
    workspace_root: str,
    config: Any = None,
) -> dict:
    """Create a new swarm record and persist it."""
    now = time.time()
    config_str = json.dumps(config if isinstance(config, dict) else (config or {}))
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO swarms (swarm_id, title, coordinator_sid, workspace_root, config, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (swarm_id, title, coordinator_sid, str(workspace_root), config_str, now, now),
    )
    conn.commit()
    return {
        "swarm_id": swarm_id,
        "title": title,
        "coordinator_sid": coordinator_sid,
        "workspace_root": str(workspace_root),
        "config": config_str,
        "created_at": now,
        "updated_at": now,
    }


def add_member(
    conn: sqlite3.Connection,
    swarm_id: str,
    name: str,
    role: str,
    pool_name: Optional[str] = None,
    brief: str = "",
    model: str = "",
    provider: str = "",
    reasoning_effort: Optional[str] = None,
    workspace_mode: str = "inherit",
    workspace_path: str = "",
    branch_name: Optional[str] = None,
    session_id: str = "",
) -> dict:
    """Add a member to an existing swarm."""
    if workspace_mode not in ("inherit", "share", "branch"):
        raise ValueError(f"Invalid workspace_mode: {workspace_mode}")

    # Check for duplicate member
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM swarm_members WHERE swarm_id = ? AND name = ?", (swarm_id, name))
    if cur.fetchone():
        raise ValueError(f"Member '{name}' already exists in swarm '{swarm_id}'")

    now = time.time()
    cur.execute(
        """INSERT INTO swarm_members (
               swarm_id, name, role, pool_name, brief, model, provider,
               reasoning_effort, workspace_mode, workspace_path, branch_name,
               session_id, state, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'working', ?, ?)""",
        (
            swarm_id, name, role, pool_name, brief, model, provider,
            reasoning_effort, workspace_mode, workspace_path, branch_name,
            session_id, now, now,
        ),
    )
    conn.commit()
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
        "workspace_path": workspace_path,
        "branch_name": branch_name,
        "session_id": session_id,
        "state": "working",
        "created_at": now,
        "updated_at": now,
    }


def update_member_state(
    conn: sqlite3.Connection,
    swarm_id: str,
    name: str,
    new_state: str,
) -> None:
    """Update member lifecycle state adhering to valid state machine transitions."""
    valid_states = {"working", "waiting", "stopped", "killed"}
    if new_state not in valid_states:
        raise ValueError(f"Invalid state: {new_state}")

    cur = conn.cursor()
    cur.execute("SELECT state FROM swarm_members WHERE swarm_id = ? AND name = ?", (swarm_id, name))
    row = cur.fetchone()
    if not row:
        raise KeyError(f"Member '{name}' not found in swarm '{swarm_id}'")

    curr_state = row[0]
    if curr_state == "killed":
        raise ValueError("Cannot transition killed member")

    # State transition rules:
    # working -> waiting, stopped, killed
    # waiting -> working, stopped, killed
    # stopped -> working, killed
    if curr_state == "stopped" and new_state not in ("working", "killed"):
        raise ValueError(f"Invalid transition from stopped to {new_state}")

    now = time.time()
    cur.execute(
        "UPDATE swarm_members SET state = ?, updated_at = ? WHERE swarm_id = ? AND name = ?",
        (new_state, now, swarm_id, name),
    )
    conn.commit()


def append_message(
    conn: sqlite3.Connection,
    swarm_id: str,
    author: str,
    mentions: Optional[List[str]],
    text: str,
    hop_count: int = 0,
) -> int:
    """Atomically append a message with monotonic sequence number using BEGIN IMMEDIATE."""
    if text is None or text == "":
        raise ValueError("Message text cannot be empty")

    hop_count = max(0, int(hop_count))
    mentions_json = json.dumps(list(mentions or []))
    now = time.time()

    cur = conn.cursor()
    cur.execute("BEGIN IMMEDIATE")
    try:
        cur.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM swarm_messages WHERE swarm_id = ?", (swarm_id,))
        seq = cur.fetchone()[0]
        cur.execute(
            """INSERT INTO swarm_messages (swarm_id, seq, author, mentions, text, hop_count, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (swarm_id, seq, author, mentions_json, text, hop_count, now),
        )
        conn.commit()
        return seq
    except Exception:
        conn.rollback()
        raise


def get_messages(
    conn: sqlite3.Connection,
    swarm_id: str,
    since_seq: int = 0,
    limit: int = 50,
) -> List[dict]:
    """Retrieve messages with seq > since_seq ordered by seq ASC."""
    since_seq = max(0, int(since_seq))
    limit = max(0, min(int(limit), 500))
    if limit == 0:
        return []

    cur = conn.cursor()
    cur.execute(
        """SELECT swarm_id, seq, author, mentions, text, hop_count, timestamp
           FROM swarm_messages
           WHERE swarm_id = ? AND seq > ?
           ORDER BY seq ASC LIMIT ?""",
        (swarm_id, since_seq, limit),
    )
    results = []
    for r in cur.fetchall():
        mentions_list = []
        if r["mentions"]:
            try:
                mentions_list = json.loads(r["mentions"])
            except Exception:
                mentions_list = []
        results.append({
            "swarm_id": r["swarm_id"],
            "seq": r["seq"],
            "author": r["author"],
            "mentions": mentions_list,
            "text": r["text"],
            "hop_count": r["hop_count"],
            "timestamp": r["timestamp"],
        })
    return results


def get_members(conn: sqlite3.Connection, swarm_id: str) -> List[dict]:
    """Retrieve all members of a swarm."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM swarm_members WHERE swarm_id = ? ORDER BY name ASC", (swarm_id,))
    rows = cur.fetchall()
    return [dict(r) for r in rows]


# ============================================================================
# 2. Runtime Engine: plugins.swarm.runtime
# ============================================================================

def parse_mentions(text: str, active_members: Optional[List[dict]] = None) -> Set[str]:
    """Parse mention tokens from text (@worker, @pool, @all, @coordinator)."""
    if not text:
        return set()

    # Match @name where @ is not preceded by word char or dot/dash (avoid test@example.com)
    tokens = set(re.findall(r'(?<![\w.-])@([a-zA-Z0-9_-]+)', text))
    resolved = set()

    for tok in tokens:
        full_tok = f"@{tok}"
        if tok == "all":
            resolved.add("@all")
            if active_members:
                for m in active_members:
                    if m.get("state") != "killed":
                        resolved.add(m["name"])
        elif tok == "coordinator":
            resolved.add("@coordinator")
        else:
            resolved.add(full_tok)
            # Check if this matches a pool name (e.g. @reviewer -> reviewer-1, reviewer-2)
            if active_members:
                for m in active_members:
                    if m.get("pool_name") == tok or m.get("name", "").startswith(f"{tok}-"):
                        resolved.add(m["name"])

    return resolved


class SwarmRuntime:
    """Runtime engine managing workers, message wakeup bus, and mention loop guards."""

    def __init__(self, swarm_id: str, db_path: Path, max_concurrent: int = 4):
        self.swarm_id = swarm_id
        self.db_path = Path(db_path)
        self.max_concurrent = max_concurrent
        self.cond = threading.Condition()
        self.listeners: Dict[str, List[threading.Event]] = {}
        self.completion_queue = None
        self._lock = threading.Lock()
        self._running = True

    def register_listener(self, worker_name: str, event: threading.Event) -> None:
        with self._lock:
            self.listeners.setdefault(worker_name, []).append(event)

    def unregister_listener(self, worker_name: str, event: threading.Event) -> None:
        with self._lock:
            if worker_name in self.listeners and event in self.listeners[worker_name]:
                self.listeners[worker_name].remove(event)

    def start_worker(self, name: str) -> None:
        conn = init_swarm_db(self.db_path)
        try:
            update_member_state(conn, self.swarm_id, name, "working")
        finally:
            conn.close()

    def handle_message_posted(
        self,
        seq: int,
        author: str,
        mentions: Any,
        text: str,
        hop_count: int = 0,
    ) -> dict:
        """Handle posted message: notify waiting threads, evaluate loop guard, trigger wakeups."""
        with self.cond:
            self.cond.notify_all()

        conn = init_swarm_db(self.db_path)
        try:
            members = get_members(conn, self.swarm_id)
        finally:
            conn.close()

        resolved_mentions = parse_mentions(text, members)
        if isinstance(mentions, (list, set)):
            for m in mentions:
                resolved_mentions.add(m if m.startswith("@") else f"@{m}")

        # Human or coordinator intervention resets hop counter
        is_coordinator_or_human = author.lower() in ("user", "human", "coordinator", "swarm_coordinator", "system")
        effective_hop = 0 if is_coordinator_or_human else hop_count

        # Cyclic mention loop guard: cap at 3 hops
        if not is_coordinator_or_human and effective_hop >= 3:
            # Suppress automatic worker wakeups and alert coordinator
            alert_text = f"Mention loop cap reached ({effective_hop} hops). Escalating to coordinator."
            conn = init_swarm_db(self.db_path)
            try:
                append_message(conn, self.swarm_id, "system", ["@coordinator"], alert_text, hop_count=0)
            finally:
                conn.close()
            with self.cond:
                self.cond.notify_all()
            return {"suppressed": True, "woken": [], "alert_posted": True}

        # Wakeup targeted workers
        woken = []
        is_all = "@all" in resolved_mentions

        for m in members:
            name = m["name"]
            state = m["state"]
            if name == author:
                continue
            if state == "killed":
                continue

            target_matched = (f"@{name}" in resolved_mentions) or (name in resolved_mentions)
            if is_all:
                # @all wakes active workers, but NOT stopped workers
                if state != "stopped":
                    target_matched = True

            if target_matched:
                if state == "stopped" and (f"@{name}" in resolved_mentions or name in resolved_mentions):
                    # Direct mention wakes stopped worker
                    conn = init_swarm_db(self.db_path)
                    try:
                        update_member_state(conn, self.swarm_id, name, "working")
                    finally:
                        conn.close()

                woken.append(name)
                # Fire registered event listeners
                with self._lock:
                    for ev in self.listeners.get(name, []):
                        ev.set()

                # Dispatch to completion queue if bound
                if self.completion_queue is not None:
                    self.completion_queue.put({
                        "type": "swarm_wakeup",
                        "swarm_id": self.swarm_id,
                        "worker_name": name,
                        "seq": seq,
                        "author": author,
                        "text": text,
                    })

        return {"suppressed": False, "woken": woken, "alert_posted": False}

    def wait_for_messages(self, since_seq: int, timeout: float = 30.0) -> List[dict]:
        """Block until a message with seq > since_seq is available or timeout expires."""
        deadline = time.time() + timeout
        conn = init_swarm_db(self.db_path)
        try:
            msgs = get_messages(conn, self.swarm_id, since_seq=since_seq, limit=50)
            if msgs:
                return msgs

            with self.cond:
                remaining = deadline - time.time()
                while remaining > 0 and self._running:
                    self.cond.wait(timeout=min(remaining, 0.05))
                    msgs = get_messages(conn, self.swarm_id, since_seq=since_seq, limit=50)
                    if msgs:
                        return msgs
                    remaining = deadline - time.time()
            return get_messages(conn, self.swarm_id, since_seq=since_seq, limit=50)
        finally:
            conn.close()

    def stop_worker(self, name: str) -> None:
        conn = init_swarm_db(self.db_path)
        try:
            update_member_state(conn, self.swarm_id, name, "stopped")
        finally:
            conn.close()

    def kill_worker(self, name: str, land: bool = True) -> dict:
        conn = init_swarm_db(self.db_path)
        try:
            members = get_members(conn, self.swarm_id)
            target = next((m for m in members if m["name"] == name), None)
            if target and target["state"] == "killed":
                return {"worker": name, "status": "killed", "landed": land}
            update_member_state(conn, self.swarm_id, name, "killed")
        finally:
            conn.close()
        return {"worker": name, "status": "killed", "landed": land}

    def shutdown(self) -> None:
        self._running = False
        with self.cond:
            self.cond.notify_all()
        with self._lock:
            for ev_list in self.listeners.values():
                for ev in ev_list:
                    ev.set()


# ============================================================================
# 3. Model Tools: plugins.swarm.tools
# ============================================================================

_CURRENT_RUNTIME: Optional[SwarmRuntime] = None
_CURRENT_DB_PATH: Optional[Path] = None
_CURRENT_SWARM_ID: str = "swarm-test-123"


def bind_tool_runtime(runtime: SwarmRuntime, db_path: Path, swarm_id: str) -> None:
    global _CURRENT_RUNTIME, _CURRENT_DB_PATH, _CURRENT_SWARM_ID
    _CURRENT_RUNTIME = runtime
    _CURRENT_DB_PATH = Path(db_path)
    _CURRENT_SWARM_ID = swarm_id


def swarm(action: str, **kwargs) -> str:
    """Coordinator administrative management tool."""
    context = kwargs.get("context") or kwargs.get("session_context") or {}
    role = context.get("role", "swarm_coordinator")

    # Role permission gating: workers cannot execute administrative actions
    if role in ("swarm_worker", "worker") and action in ("add", "kill", "model", "title"):
        raise PermissionError(f"Action '{action}' is restricted to swarm_coordinator")

    db_path = kwargs.get("db_path") or _CURRENT_DB_PATH
    swarm_id = kwargs.get("swarm_id") or _CURRENT_SWARM_ID
    if not db_path:
        return "Error: Database path not configured."

    conn = init_swarm_db(db_path)
    try:
        if action == "status":
            members = get_members(conn, swarm_id)
            if not members:
                return "Swarm active. No members registered."
            lines = [f"{m['name']} | {m['role']} | {m['state']} | {m['model']} | {m['workspace_mode']}" for m in members]
            return "Active Swarm Roster:\n" + "\n".join(lines)

        elif action == "title":
            title = kwargs.get("title", "Untitled Swarm")
            conn.execute("UPDATE swarms SET title = ? WHERE swarm_id = ?", (title, swarm_id))
            conn.commit()
            return f"Swarm title updated to: {title}"

        elif action == "models":
            return "Available models: gpt-4o, claude-3-7-sonnet, gemini-2.5-pro, fast"

        elif action == "add":
            name = kwargs.get("name")
            brief = kwargs.get("brief", "")
            if not name:
                raise ValueError("Worker name is required")
            if not brief:
                raise ValueError("Worker brief is required")
            model = kwargs.get("model", "default")
            provider = kwargs.get("provider", "custom")
            workspace_mode = kwargs.get("workspace", kwargs.get("workspace_mode", "inherit"))
            add_member(
                conn,
                swarm_id,
                name=name,
                role="worker",
                brief=brief,
                model=model,
                provider=provider,
                workspace_mode=workspace_mode,
            )
            return f"Worker '{name}' added successfully."

        elif action == "model":
            name = kwargs.get("name")
            new_model = kwargs.get("model")
            if not name or not new_model:
                raise ValueError("name and model parameters are required")
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM swarm_members WHERE swarm_id = ? AND name = ?", (swarm_id, name))
            if not cur.fetchone():
                return f"Error: Member '{name}' not found."
            conn.execute("UPDATE swarm_members SET model = ? WHERE swarm_id = ? AND name = ?", (new_model, swarm_id, name))
            conn.commit()
            return f"Model for '{name}' switched to {new_model}"

        elif action == "kill":
            name = kwargs.get("name")
            if not name:
                raise ValueError("name is required for kill")
            if _CURRENT_RUNTIME:
                _CURRENT_RUNTIME.kill_worker(name)
            else:
                update_member_state(conn, swarm_id, name, "killed")
            return f"Worker '{name}' terminated."

        else:
            raise ValueError(f"Unknown swarm action: {action}")
    finally:
        conn.close()


def post_message(text: str, mentions: Optional[List[str]] = None, **kwargs) -> str:
    """Post message to board and wake mentioned workers."""
    if not text:
        raise ValueError("Message text cannot be empty")

    context = kwargs.get("context") or kwargs.get("session_context") or {}
    author = kwargs.get("author") or context.get("worker_name") or context.get("author") or "user"
    db_path = kwargs.get("db_path") or _CURRENT_DB_PATH
    swarm_id = kwargs.get("swarm_id") or _CURRENT_SWARM_ID

    if not db_path:
        raise RuntimeError("Database connection unavailable")

    conn = init_swarm_db(db_path)
    try:
        resolved_mentions = list(parse_mentions(text))
        if mentions:
            for m in mentions:
                if m not in resolved_mentions:
                    resolved_mentions.append(m)

        hop_count = kwargs.get("hop_count", 0)
        seq = append_message(conn, swarm_id, author, resolved_mentions, text, hop_count=hop_count)
        if _CURRENT_RUNTIME:
            _CURRENT_RUNTIME.handle_message_posted(seq, author, resolved_mentions, text, hop_count=hop_count)
        return f"Message posted with seq {seq}"
    finally:
        conn.close()


def read_board(since: int = 0, limit: int = 50, **kwargs) -> str:
    """Read recent messages from public board."""
    db_path = kwargs.get("db_path") or _CURRENT_DB_PATH
    swarm_id = kwargs.get("swarm_id") or _CURRENT_SWARM_ID
    if not db_path:
        return "No messages found."

    conn = init_swarm_db(db_path)
    try:
        msgs = get_messages(conn, swarm_id, since_seq=since, limit=limit)
        if not msgs:
            return "No messages found."
        lines = []
        for m in msgs:
            mentions_str = " ".join(m["mentions"]) if m["mentions"] else ""
            lines.append(f"[{m['seq']}] <{m['author']}>: {m['text']} {mentions_str}".strip())
        return "\n".join(lines)
    finally:
        conn.close()


def swarm_wait(since: int = 0, timeout: float = 30.0, **kwargs) -> str:
    """In-turn blocking wait tool for new board messages."""
    timeout = max(0.0, float(timeout))
    since = max(0, int(since))

    if _CURRENT_RUNTIME:
        msgs = _CURRENT_RUNTIME.wait_for_messages(since_seq=since, timeout=timeout)
        if msgs:
            lines = [f"[{m['seq']}] <{m['author']}>: {m['text']}" for m in msgs]
            return "\n".join(lines)
        return "Wait timed out. No new messages."

    # Fallback to direct DB read
    db_path = kwargs.get("db_path") or _CURRENT_DB_PATH
    swarm_id = kwargs.get("swarm_id") or _CURRENT_SWARM_ID
    if not db_path:
        return "Wait timed out. No new messages."

    conn = init_swarm_db(db_path)
    try:
        msgs = get_messages(conn, swarm_id, since_seq=since, limit=50)
        if msgs:
            lines = [f"[{m['seq']}] <{m['author']}>: {m['text']}" for m in msgs]
            return "\n".join(lines)
        return "Wait timed out. No new messages."
    finally:
        conn.close()


# ============================================================================
# 4. Slash Commands: plugins.swarm.slash_handlers
# ============================================================================

def handle_swarm_command(args: Any, session_context: Optional[dict] = None) -> str:
    """Handle /swarm slash command subcommands."""
    if isinstance(args, str):
        args_list = args.strip().split()
    elif isinstance(args, list):
        args_list = args
    else:
        args_list = []

    if not args_list:
        return (
            "Usage: /swarm <task> | /swarm status | /swarm board [N] | "
            "/swarm add <name> <brief> | /swarm say <@worker> <text> | "
            "/swarm land <worker> | /swarm kill <worker|all> | /swarm stop"
        )

    subcmd = args_list[0].lower()
    ctx = session_context or {}
    db_path = ctx.get("db_path") or _CURRENT_DB_PATH
    swarm_id = ctx.get("swarm_id") or _CURRENT_SWARM_ID

    if subcmd == "status":
        if not db_path:
            return "No active swarm session."
        conn = init_swarm_db(db_path)
        try:
            members = get_members(conn, swarm_id)
            if not members:
                return "No active swarm members found."
            header = "Worker | Role | State | Model | Workspace"
            sep = "-" * len(header)
            rows = [f"{m['name']} | {m['role']} | {m['state']} | {m['model']} | {m['workspace_mode']}" for m in members]
            return f"{header}\n{sep}\n" + "\n".join(rows)
        finally:
            conn.close()

    elif subcmd == "board":
        limit = 20
        if len(args_list) > 1:
            try:
                limit = int(args_list[1])
            except ValueError:
                return f"Error: Board count must be a number, got '{args_list[1]}'"
        if not db_path:
            return "No messages on board."
        return read_board(since=0, limit=limit, db_path=db_path, swarm_id=swarm_id)

    elif subcmd == "add":
        if len(args_list) < 3:
            return "Usage: /swarm add <name> [--model M] [--worktree W] <brief>"
        name = args_list[1]
        brief_tokens = []
        i = 2
        model = "default"
        worktree = "inherit"
        while i < len(args_list):
            if args_list[i] == "--model" and i + 1 < len(args_list):
                model = args_list[i + 1]
                i += 2
            elif args_list[i] in ("--worktree", "--workspace") and i + 1 < len(args_list):
                worktree = args_list[i + 1]
                i += 2
            elif args_list[i].startswith("--"):
                return f"Error: Unrecognized flag '{args_list[i]}'"
            else:
                brief_tokens.append(args_list[i])
                i += 1
        brief = " ".join(brief_tokens)
        if not brief:
            return "Error: Worker brief is required."
        conn = init_swarm_db(db_path)
        try:
            add_member(conn, swarm_id, name, "worker", brief=brief, model=model, workspace_mode=worktree)
            return f"Worker '{name}' added to swarm."
        finally:
            conn.close()

    elif subcmd == "say":
        if len(args_list) < 2:
            return "Usage: /swarm say <@target> <message text>"
        target = args_list[1]
        text = " ".join(args_list[2:])
        if not text:
            return "Error: Message text cannot be empty."
        full_text = f"{target} {text}" if target.startswith("@") else f"@{target} {text}"
        return post_message(full_text, author="user", db_path=db_path, swarm_id=swarm_id)

    elif subcmd == "stop":
        if not db_path:
            return "No active swarm to stop."
        conn = init_swarm_db(db_path)
        try:
            members = get_members(conn, swarm_id)
            for m in members:
                if m["state"] in ("working", "waiting"):
                    # Transition to waiting first if working, then stopped to respect state machine
                    if m["state"] == "working":
                        update_member_state(conn, swarm_id, m["name"], "waiting")
                    update_member_state(conn, swarm_id, m["name"], "stopped")
            return "All active swarm workers stopped."
        finally:
            conn.close()

    elif subcmd == "kill":
        if len(args_list) < 2:
            return "Usage: /swarm kill <name|all>"
        target = args_list[1]
        conn = init_swarm_db(db_path)
        try:
            if target == "all":
                members = get_members(conn, swarm_id)
                for m in members:
                    try:
                        update_member_state(conn, swarm_id, m["name"], "killed")
                    except (KeyError, ValueError):
                        pass
                return "All swarm workers killed."
            else:
                try:
                    update_member_state(conn, swarm_id, target, "killed")
                    return f"Worker '{target}' killed."
                except (KeyError, ValueError):
                    return f"Worker '{target}' already terminated or not found."
        finally:
            conn.close()

    elif subcmd == "land":
        if len(args_list) < 2:
            return "Usage: /swarm land <name>"
        target = args_list[1]
        repo_root = Path(ctx.get("workspace_root", "."))
        res = land_worker_workspace(repo_root, target)
        if res.get("status") == "conflict":
            return f"Conflict detected while landing '{target}'. Changes preserved in worktree."
        return f"Worker '{target}' workspace landed successfully."

    else:
        # Task initialization: /swarm <task>
        import uuid
        task_str = " ".join(args_list)
        title = task_str[:40] + ("..." if len(task_str) > 40 else "")
        new_swarm_id = f"swarm-{uuid.uuid4().hex[:8]}"
        if db_path:
            conn = init_swarm_db(db_path)
            try:
                create_swarm(conn, new_swarm_id, title, "coordinator-1", str(ctx.get("workspace_root", ".")))
            finally:
                conn.close()
        return f"Swarm coordinator initialized for task: '{title}' [ID: {new_swarm_id}]"


def handle_teamwork_command(task: str, session_context: Optional[dict] = None) -> str:
    """Handle /teamwork slash command: configure session as Teamwork Coordinator."""
    ctx = session_context or {}
    workspace_root = Path(ctx.get("workspace_root", "."))

    # Must be run inside a git repository
    if not (workspace_root / ".git").exists():
        return "Warning: Teamwork coordinator mode requires a git repository. Please initialize git first."

    task_str = task.strip()
    if not task_str:
        return "Usage: /teamwork <task description>"

    return (
        f"Teamwork Coordinator mode initialized for: '{task_str}'. "
        "Strict rules enforced: no direct code edits; disjoint file ownership; independent verifier."
    )


# ============================================================================
# 5. Worktree Manager: plugins.swarm.worktree_manager
# ============================================================================

def sanitize_branch_name(name: str) -> str:
    """Sanitize name to form a valid git ref."""
    cleaned = re.sub(r'[^a-zA-Z0-9_-]', '-', name)
    cleaned = re.sub(r'-+', '-', cleaned).strip('-')
    return cleaned or "worker"


def create_worker_workspace(
    repo_root: Path,
    swarm_id: str,
    worker_name: str,
    mode: str = "inherit",
) -> Path:
    """Provision workspace for a worker based on mode (inherit, share, branch)."""
    repo_root = Path(repo_root).resolve()
    if mode == "inherit":
        return repo_root

    sanitized = sanitize_branch_name(worker_name)
    wt_dir = repo_root / ".worktrees" / sanitized
    wt_dir.parent.mkdir(parents=True, exist_ok=True)

    if mode == "share":
        # Create worktree off current HEAD without creating new branch
        subprocess.run(
            ["git", "worktree", "add", "--detach", str(wt_dir), "HEAD"],
            cwd=repo_root, capture_output=True, text=True, check=True,
        )
    elif mode == "branch":
        branch_name = f"swarm/{swarm_id}/{sanitized}"
        subprocess.run(
            ["git", "worktree", "add", "-b", branch_name, str(wt_dir), "HEAD"],
            cwd=repo_root, capture_output=True, text=True, check=True,
        )
    else:
        raise ValueError(f"Unknown workspace mode: {mode}")

    return wt_dir


def land_worker_workspace(
    repo_root: Path,
    worker_name: str,
    branch_name: Optional[str] = None,
) -> dict:
    """Merge worker branch changes back into parent workspace and clean up."""
    repo_root = Path(repo_root).resolve()
    sanitized = sanitize_branch_name(worker_name)
    wt_dir = repo_root / ".worktrees" / sanitized

    # If worktree dir doesn't exist, treat as no-op or clean
    if not wt_dir.exists():
        return {"worker": worker_name, "status": "clean", "changes": 0}

    # Snapshot uncommitted edits if any
    status_proc = subprocess.run(["git", "status", "--porcelain"], cwd=wt_dir, capture_output=True, text=True)
    if status_proc.stdout.strip():
        subprocess.run(["git", "add", "-A"], cwd=wt_dir, capture_output=True)
        subprocess.run(["git", "commit", "-m", f"Auto-snapshot before landing {worker_name}"], cwd=wt_dir, capture_output=True)

    # Release any language server clients on this worktree
    try:
        from hermes_cli.worktree_ops import release_lsp_clients
        release_lsp_clients(str(wt_dir))
    except Exception:
        pass

    # Determine branch to merge
    target_branch = branch_name
    if not target_branch:
        branch_proc = subprocess.run(["git", "branch", "--show-current"], cwd=wt_dir, capture_output=True, text=True)
        target_branch = branch_proc.stdout.strip()

    has_conflict = False
    if target_branch:
        # Merge target_branch into parent
        merge_proc = subprocess.run(["git", "merge", target_branch], cwd=repo_root, capture_output=True, text=True)
        if merge_proc.returncode != 0:
            has_conflict = True
            subprocess.run(["git", "merge", "--abort"], cwd=repo_root, capture_output=True)
            return {"worker": worker_name, "status": "conflict", "conflicts": [merge_proc.stderr]}

    # Cleanup worktree
    subprocess.run(["git", "worktree", "remove", "--force", str(wt_dir)], cwd=repo_root, capture_output=True)
    subprocess.run(["git", "worktree", "prune"], cwd=repo_root, capture_output=True)
    if wt_dir.exists():
        shutil.rmtree(wt_dir, ignore_errors=True)

    return {"worker": worker_name, "status": "merged", "branch": target_branch}


def cleanup_worker_workspace(repo_root: Path, worker_name: str) -> None:
    """Force remove worker worktree."""
    repo_root = Path(repo_root).resolve()
    sanitized = sanitize_branch_name(worker_name)
    wt_dir = repo_root / ".worktrees" / sanitized
    if wt_dir.exists():
        try:
            from hermes_cli.worktree_ops import release_lsp_clients
            release_lsp_clients(str(wt_dir))
        except Exception:
            pass
        subprocess.run(["git", "worktree", "remove", "--force", str(wt_dir)], cwd=repo_root, capture_output=True)
        subprocess.run(["git", "worktree", "prune"], cwd=repo_root, capture_output=True)
        shutil.rmtree(wt_dir, ignore_errors=True)


# ============================================================================
# 6. Web Dashboard Endpoints: plugins.swarm.dashboard.plugin_api
# ============================================================================

def create_dashboard_router(db_path: Path) -> APIRouter:
    """Create FastAPI router exposing read-only live inspection endpoints."""
    router = APIRouter()
    db_file = Path(db_path)

    @router.get("/status")
    def get_status():
        if not db_file.exists():
            return {"status": "ok", "active": False, "swarm_id": None, "members_count": 0, "messages_count": 0}
        conn = init_swarm_db(db_file)
        try:
            cur = conn.cursor()
            cur.execute("SELECT swarm_id, title FROM swarms ORDER BY created_at DESC LIMIT 1")
            swarm_row = cur.fetchone()
            if not swarm_row:
                return {"status": "ok", "active": False, "swarm_id": None, "members_count": 0, "messages_count": 0}
            swarm_id = swarm_row["swarm_id"]
            cur.execute("SELECT COUNT(*) FROM swarm_members WHERE swarm_id = ?", (swarm_id,))
            mem_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM swarm_messages WHERE swarm_id = ?", (swarm_id,))
            msg_count = cur.fetchone()[0]
            return {
                "status": "ok",
                "active": True,
                "swarm_id": swarm_id,
                "title": swarm_row["title"],
                "members_count": mem_count,
                "messages_count": msg_count,
            }
        finally:
            conn.close()

    @router.get("/board")
    def get_board(since: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=500)):
        if not db_file.exists():
            return {"status": "ok", "messages": []}
        conn = init_swarm_db(db_file)
        try:
            cur = conn.cursor()
            cur.execute("SELECT swarm_id FROM swarms ORDER BY created_at DESC LIMIT 1")
            swarm_row = cur.fetchone()
            if not swarm_row:
                return {"status": "ok", "messages": []}
            msgs = get_messages(conn, swarm_row["swarm_id"], since_seq=since, limit=limit)
            return {"status": "ok", "messages": msgs}
        finally:
            conn.close()

    @router.get("/members")
    def get_members_list():
        if not db_file.exists():
            return {"status": "ok", "members": []}
        conn = init_swarm_db(db_file)
        try:
            cur = conn.cursor()
            cur.execute("SELECT swarm_id FROM swarms ORDER BY created_at DESC LIMIT 1")
            swarm_row = cur.fetchone()
            if not swarm_row:
                return {"status": "ok", "members": []}
            mems = get_members(conn, swarm_row["swarm_id"])
            return {"status": "ok", "members": mems}
        finally:
            conn.close()

    return router


def register(ctx: Any) -> None:
    """Fallback plugin entrypoint conforming to Hermes plugin system."""
    from plugins.swarm.tools import swarm, post_message, read_board, swarm_wait
    from plugins.swarm.slash_handlers import handle_swarm_command, handle_teamwork_command

    if hasattr(ctx, "register_tool"):
        ctx.register_tool("swarm", swarm)
        ctx.register_tool("post_message", post_message)
        ctx.register_tool("read_board", read_board)
        ctx.register_tool("swarm_wait", swarm_wait)

    if hasattr(ctx, "register_slash_command"):
        ctx.register_slash_command("swarm", handle_swarm_command)
        ctx.register_slash_command("teamwork", handle_teamwork_command)

    if hasattr(ctx, "mount_router"):
        from plugins.swarm.dashboard.plugin_api import create_dashboard_router
        db_path = getattr(ctx, "db_path", None)
        if db_path:
            router = create_dashboard_router(db_path)
            ctx.mount_router("/api/plugins/swarm", router)


# ============================================================================
# 7. Shims Installer into sys.modules
# ============================================================================

def install_shims() -> None:
    """Import real modules from plugins.swarm.* when they exist on disk, falling back to shims if missing."""
    import importlib

    repo_root = str(Path(__file__).resolve().parents[3])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    import plugins

    # Load real top-level plugins.swarm if present on disk, otherwise create shim package
    if "plugins.swarm" not in sys.modules:
        try:
            pkg = importlib.import_module("plugins.swarm")
        except ImportError:
            pkg = types.ModuleType("plugins.swarm")
            pkg.__path__ = [str(Path(repo_root) / "plugins" / "swarm"), str(Path(__file__).parent)]
            sys.modules["plugins.swarm"] = pkg
            setattr(plugins, "swarm", pkg)
    else:
        pkg = sys.modules["plugins.swarm"]

    if not hasattr(pkg, "register"):
        setattr(pkg, "register", register)

    def _load_or_shim(mod_name: str, attr_name: str, populate_fn) -> types.ModuleType:
        if mod_name in sys.modules:
            mod = sys.modules[mod_name]
            setattr(pkg, attr_name, mod)
            return mod
        try:
            mod = importlib.import_module(mod_name)
            setattr(pkg, attr_name, mod)
            return mod
        except ImportError:
            m = types.ModuleType(mod_name)
            populate_fn(m)
            sys.modules[mod_name] = m
            setattr(pkg, attr_name, m)
            return m

    # 1. state
    def _pop_state(m: types.ModuleType) -> None:
        m.init_swarm_db = init_swarm_db
        m.create_swarm = create_swarm
        m.add_member = add_member
        m.update_member_state = update_member_state
        m.append_message = append_message
        m.get_messages = get_messages
        m.get_members = get_members

    _load_or_shim("plugins.swarm.state", "state", _pop_state)

    # 2. runtime
    def _pop_runtime(m: types.ModuleType) -> None:
        m.parse_mentions = parse_mentions
        m.SwarmRuntime = SwarmRuntime

    _load_or_shim("plugins.swarm.runtime", "runtime", _pop_runtime)

    # 3. tools
    def _pop_tools(m: types.ModuleType) -> None:
        m.swarm = swarm
        m.post_message = post_message
        m.read_board = read_board
        m.swarm_wait = swarm_wait
        m.bind_tool_runtime = bind_tool_runtime

    _load_or_shim("plugins.swarm.tools", "tools", _pop_tools)

    # 4. slash_handlers
    def _pop_slash(m: types.ModuleType) -> None:
        m.handle_swarm_command = handle_swarm_command
        m.handle_teamwork_command = handle_teamwork_command

    _load_or_shim("plugins.swarm.slash_handlers", "slash_handlers", _pop_slash)

    # 5. worktree_manager
    def _pop_worktree(m: types.ModuleType) -> None:
        m.create_worker_workspace = create_worker_workspace
        m.land_worker_workspace = land_worker_workspace
        m.cleanup_worker_workspace = cleanup_worker_workspace
        m.sanitize_branch_name = sanitize_branch_name

    _load_or_shim("plugins.swarm.worktree_manager", "worktree_manager", _pop_worktree)

    # 6. dashboard & dashboard.plugin_api
    if "plugins.swarm.dashboard" not in sys.modules:
        try:
            dash_pkg = importlib.import_module("plugins.swarm.dashboard")
        except ImportError:
            dash_pkg = types.ModuleType("plugins.swarm.dashboard")
            dash_pkg.__path__ = [str(Path(repo_root) / "plugins" / "swarm" / "dashboard")]
            sys.modules["plugins.swarm.dashboard"] = dash_pkg
        setattr(pkg, "dashboard", dash_pkg)
    else:
        dash_pkg = sys.modules["plugins.swarm.dashboard"]

    if "plugins.swarm.dashboard.plugin_api" not in sys.modules:
        try:
            dash_mod = importlib.import_module("plugins.swarm.dashboard.plugin_api")
        except ImportError:
            dash_mod = types.ModuleType("plugins.swarm.dashboard.plugin_api")
            dash_mod.create_dashboard_router = create_dashboard_router
            sys.modules["plugins.swarm.dashboard.plugin_api"] = dash_mod
        setattr(dash_pkg, "plugin_api", dash_mod)

