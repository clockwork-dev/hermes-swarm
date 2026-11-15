"""Swarm Model Tools & Role-Based Permission Gating.

Implements the four model-facing swarm tools:
- `swarm`: Coordinator administrative management tool (status, models, add, model, kill, title).
- `post_message`: Append message to the shared board and wake mentioned workers.
- `read_board`: Read recent messages from the shared board since a sequence number.
- `swarm_wait`: In-turn condition-variable wait for new board messages.

Enforces role-based permission gating so workers cannot execute administrative actions.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .runtime import SwarmRuntime, parse_mentions, seed_worker_context
from .state import (
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
from .worktree_manager import create_worker_workspace

_CURRENT_RUNTIME: Optional[SwarmRuntime] = None
_CURRENT_DB_PATH: Optional[Path] = None
_CURRENT_SWARM_ID: str = "swarm-test-123"

COORDINATOR_TOOLS: Tuple[str, ...] = ("swarm", "post_message", "read_board", "swarm_wait")
WORKER_TOOLS: Tuple[str, ...] = ("post_message", "read_board", "swarm_wait")

SWARM_SCHEMA: Dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "swarm",
        "description": (
            "Manage the swarm of agents working on the shared message board. "
            "Actions: status, models, add, model, kill, title."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["status", "models", "add", "model", "kill", "title"],
                    "description": "Administrative action to perform.",
                },
                "name": {
                    "type": "string",
                    "description": "Roster name of the worker (for add, model, kill).",
                },
                "brief": {
                    "type": "string",
                    "description": "The worker's assignment and completion criteria (for add).",
                },
                "model": {
                    "type": "string",
                    "description": "Model identifier to assign or switch to (for add, model).",
                },
                "provider": {
                    "type": "string",
                    "description": "Model provider identifier (for add).",
                },
                "reasoning_effort": {
                    "type": "string",
                    "description": "Optional reasoning effort level (for add).",
                },
                "workspace": {
                    "type": "string",
                    "enum": ["inherit", "share", "branch"],
                    "description": "Workspace isolation mode for the worker (for add).",
                },
                "n": {
                    "type": "integer",
                    "description": "Optional number of homogeneous worker copies to spawn in a pool.",
                },
                "tasks": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional per-worker slice targets when spawning a sharded group.",
                },
                "title": {
                    "type": "string",
                    "description": "Short title for the swarm (for title).",
                },
                "all": {
                    "type": "boolean",
                    "description": "List all available models (for models).",
                },
            },
            "required": ["action"],
        },
    },
}

POST_MESSAGE_SCHEMA: Dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "post_message",
        "description": "Post a message to the shared swarm board and wake mentioned workers.",
        "parameters": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Message text to post on the board. May include @mentions.",
                },
                "mentions": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional explicit list of @mentions to notify.",
                },
            },
            "required": ["text"],
        },
    },
}

READ_BOARD_SCHEMA: Dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "read_board",
        "description": "Read messages from the shared swarm board after a sequence number.",
        "parameters": {
            "type": "object",
            "properties": {
                "since": {
                    "type": "integer",
                    "description": "Return messages with seq > since (default 0).",
                    "default": 0,
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of messages to return (default 50, max 500).",
                    "default": 50,
                },
            },
            "required": [],
        },
    },
}

SWARM_WAIT_SCHEMA: Dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "swarm_wait",
        "description": "Block until new messages appear on the swarm board after since_seq or timeout expires.",
        "parameters": {
            "type": "object",
            "properties": {
                "since": {
                    "type": "integer",
                    "description": "Wait for messages with seq > since (default 0).",
                    "default": 0,
                },
                "timeout": {
                    "type": "number",
                    "description": "Maximum seconds to wait before returning (default 30.0).",
                    "default": 30.0,
                },
            },
            "required": [],
        },
    },
}

TOOL_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "swarm": SWARM_SCHEMA,
    "post_message": POST_MESSAGE_SCHEMA,
    "read_board": READ_BOARD_SCHEMA,
    "swarm_wait": SWARM_WAIT_SCHEMA,
}


def register_swarm_toolsets() -> None:
    """No-op hook retained for backwards compatibility; toolsets are registered via ctx.register_tool."""
    return None


def set_default_db_path(db_path: Path | str | None) -> None:
    """Set default SQLite database path if not already configured."""
    global _CURRENT_DB_PATH
    if _CURRENT_DB_PATH is None and db_path is not None:
        _CURRENT_DB_PATH = Path(db_path)


def set_active_swarm(swarm_id: str, db_path: Path | str | None = None) -> None:
    """Update the active swarm_id and optionally the default SQLite database path."""
    global _CURRENT_SWARM_ID, _CURRENT_DB_PATH, _CURRENT_RUNTIME
    _CURRENT_SWARM_ID = str(swarm_id)
    if db_path is not None:
        _CURRENT_DB_PATH = Path(db_path)
    if _CURRENT_DB_PATH is not None:
        _CURRENT_RUNTIME = SwarmRuntime(_CURRENT_SWARM_ID, _CURRENT_DB_PATH)


def bind_tool_runtime(
    runtime: Optional[SwarmRuntime],
    db_path: Path | str | None,
    swarm_id: str,
) -> None:
    """Bind the active SwarmRuntime, SQLite database path, and swarm_id to the tools module."""
    global _CURRENT_RUNTIME, _CURRENT_DB_PATH, _CURRENT_SWARM_ID
    _CURRENT_RUNTIME = runtime
    _CURRENT_DB_PATH = Path(db_path) if db_path is not None else None
    _CURRENT_SWARM_ID = str(swarm_id)


def get_tool_runtime() -> Tuple[Optional[SwarmRuntime], Optional[Path], str]:
    """Return the currently bound (runtime, db_path, swarm_id) tuple."""
    return _CURRENT_RUNTIME, _CURRENT_DB_PATH, _CURRENT_SWARM_ID


def _extract_context(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Extract session/caller context dictionary from tool kwargs."""
    ctx = kwargs.get("context") or kwargs.get("session_context")
    if isinstance(ctx, dict):
        return ctx
    return {}


def _extract_caller_role(kwargs: Dict[str, Any]) -> str:
    """Determine caller role from context, session_context, or agent instance."""
    ctx = _extract_context(kwargs)
    if "role" in ctx and ctx["role"]:
        return str(ctx["role"])
    agent = kwargs.get("agent")
    if agent is not None:
        agent_role = getattr(agent, "swarm_role", None) or getattr(agent, "role", None)
        if agent_role:
            return str(agent_role)
    if "role" in kwargs and kwargs["role"]:
        return str(kwargs["role"])
    return "swarm_coordinator"


def _resolve_db_and_swarm(kwargs: Dict[str, Any]) -> Tuple[Optional[Path], str]:
    """Resolve effective database path and swarm_id from kwargs, bound state, or context."""
    ctx = _extract_context(kwargs)
    raw_db = kwargs.get("db_path") or _CURRENT_DB_PATH or ctx.get("db_path")
    db_path = Path(raw_db) if raw_db is not None else None
    swarm_id = str(kwargs.get("swarm_id") or _CURRENT_SWARM_ID or ctx.get("swarm_id") or "swarm-test-123")
    return db_path, swarm_id


def _ensure_swarm_and_runtime(conn: Any, db_path: Path, swarm_id: str) -> SwarmRuntime:
    """Ensure the swarm record exists in SQLite and a live SwarmRuntime is bound."""
    global _CURRENT_RUNTIME, _CURRENT_DB_PATH, _CURRENT_SWARM_ID
    if get_swarm(conn, swarm_id) is None:
        create_swarm(conn, swarm_id, "Active Swarm", "coordinator", os.getcwd())
    if (
        _CURRENT_RUNTIME is None
        or _CURRENT_DB_PATH is None
        or Path(db_path) != _CURRENT_DB_PATH
        or swarm_id != _CURRENT_SWARM_ID
    ):
        _CURRENT_RUNTIME = SwarmRuntime(swarm_id, db_path)
        _CURRENT_DB_PATH = Path(db_path)
        _CURRENT_SWARM_ID = str(swarm_id)
    return _CURRENT_RUNTIME


def _spawn_live_worker_turn(
    runtime: SwarmRuntime,
    db_path: Path,
    swarm_id: str,
    worker_name: str,
    trigger_text: str,
    parent_agent: Any = None,
) -> None:
    """Execute an asynchronous worker turn on SwarmRuntime's executor using a child AIAgent."""
    if parent_agent is not None and callable(getattr(parent_agent, "run_conversation", None)):
        agent_cls = type(parent_agent)
    else:
        run_agent_mod = sys.modules.get("run_agent")
        agent_cls = getattr(run_agent_mod, "AIAgent", None) if run_agent_mod is not None else None
    if agent_cls is None:
        return

    def _worker_task() -> None:
        conn = init_swarm_db(db_path)
        try:
            member = get_member(conn, swarm_id, worker_name)
            if not member or member.get("state") == "killed":
                return
            board_msgs = get_messages(conn, swarm_id, since_seq=0, limit=30)
            last_seq_before = board_msgs[-1]["seq"] if board_msgs else 0
        finally:
            conn.close()

        brief = member.get("brief", "")
        ws_path = member.get("workspace_path") or os.getcwd()
        seeded = seed_worker_context(worker_name, brief, board_messages=board_msgs)
        sys_prompt = seeded[-1]["content"] if seeded else f"You are swarm worker '{worker_name}'. Brief: {brief}"
        if ws_path:
            sys_prompt += f"\nWorking Directory: {ws_path}"

        worker_model = member.get("model")
        if not worker_model or worker_model == "default":
            worker_model = getattr(parent_agent, "model", None) if parent_agent is not None else None
        worker_provider = member.get("provider")
        if not worker_provider or worker_provider == "custom":
            worker_provider = getattr(parent_agent, "provider", None) if parent_agent is not None else None

        if not worker_model or not worker_provider:
            try:
                from hermes_cli.config import load_config_readonly
                cfg = load_config_readonly() or {}
                m_cfg = cfg.get("model") if isinstance(cfg.get("model"), dict) else {}
                if not worker_model:
                    worker_model = m_cfg.get("model") or m_cfg.get("default")
                if not worker_provider:
                    worker_provider = m_cfg.get("provider")
            except Exception:
                pass
        child = None
        try:
            child = agent_cls(
                model=worker_model,
                provider=worker_provider,
                api_key=getattr(parent_agent, "api_key", None),
                base_url=getattr(parent_agent, "base_url", None),
                max_iterations=15,
                quiet_mode=True,
                side_agent=True,
                skip_context_files=True,
                skip_memory=True,
                ephemeral_system_prompt=sys_prompt,
                log_prefix=f"[swarm-{worker_name}]",
                platform="subagent",
            )
            child.swarm_member_name = worker_name
            user_prompt = (
                f"New board message addressing you (@{worker_name}):\n{trigger_text}\n\n"
                f"Complete your assigned task ({brief}) and either call `post_message` to report to `@coordinator` "
                "or return a concise final summary."
            )
            res = child.run_conversation(user_prompt)
            final_text = str((res or {}).get("final_response") or "").strip()

            conn2 = init_swarm_db(db_path)
            try:
                recent = get_messages(conn2, swarm_id, since_seq=last_seq_before, limit=50)
                already_posted = any(m.get("author") == worker_name for m in recent)
                if not already_posted and final_text:
                    reply_text = f"@coordinator {final_text}" if "@coordinator" not in final_text else final_text
                    seq = append_message(conn2, swarm_id, worker_name, ["@coordinator"], reply_text, hop_count=1)
                    with runtime.cond:
                        runtime.cond.notify_all()
                cur_mem = get_member(conn2, swarm_id, worker_name)
                if cur_mem and cur_mem.get("state") == "working":
                    update_member_state(conn2, swarm_id, worker_name, "waiting")
            finally:
                conn2.close()
        except Exception as exc:
            conn_err = init_swarm_db(db_path)
            try:
                append_message(
                    conn_err,
                    swarm_id,
                    worker_name,
                    ["@coordinator"],
                    f"@coordinator Worker '{worker_name}' encountered an error: {exc}",
                    hop_count=1,
                )
                cur_mem = get_member(conn_err, swarm_id, worker_name)
                if cur_mem and cur_mem.get("state") == "working":
                    update_member_state(conn_err, swarm_id, worker_name, "waiting")
            except Exception:
                pass
            finally:
                conn_err.close()
            with runtime.cond:
                runtime.cond.notify_all()
        finally:
            if child is not None and callable(getattr(child, "close", None)):
                try:
                    child.close()
                except Exception:
                    pass

    runtime.start_worker(worker_name, _worker_task)


def swarm(action: Any = "", **kwargs: Any) -> str:
    """Coordinator administrative management tool."""
    if isinstance(action, dict):
        merged = dict(action)
        merged.update(kwargs)
        action = merged.pop("action", "")
        kwargs = merged
    role = _extract_caller_role(kwargs)

    # Role permission gating: workers cannot execute administrative actions
    if role in ("swarm_worker", "worker") and action in ("add", "kill", "model", "title"):
        raise PermissionError(f"Action '{action}' is restricted to swarm_coordinator")

    db_path, swarm_id = _resolve_db_and_swarm(kwargs)
    if not db_path:
        return "Error: Database path not configured."

    conn = init_swarm_db(db_path)
    try:
        if action == "status":
            members = get_members(conn, swarm_id)
            if not members:
                return "Swarm active. No members registered."
            lines = [
                f"{m['name']} | {m['role']} | {m['state']} | {m['model']} | {m['workspace_mode']}"
                for m in members
            ]
            return "Active Swarm Roster:\n" + "\n".join(lines)

        elif action == "title":
            _ensure_swarm_and_runtime(conn, Path(db_path), swarm_id)
            title = kwargs.get("title", "Untitled Swarm")
            if not title or not str(title).strip():
                title = "Untitled Swarm"
            with write_txn(conn):
                conn.execute("UPDATE swarms SET title = ? WHERE swarm_id = ?", (str(title), swarm_id))
            return f"Swarm title updated to: {title}"

        elif action == "models":
            base_models = ["gpt-4o", "claude-3-7-sonnet", "gemini-2.5-pro", "fast"]
            if kwargs.get("all"):
                extra_models = ["claude-3-5-haiku", "o3-mini", "gemini-2.5-flash"]
                for em in extra_models:
                    if em not in base_models:
                        base_models.append(em)
            return "Available models: " + ", ".join(base_models)

        elif action == "add":
            name = kwargs.get("name")
            brief = kwargs.get("brief", "")
            if not name or not str(name).strip():
                raise ValueError("Worker name is required")
            if not brief or not str(brief).strip():
                raise ValueError("Worker brief is required")

            _ensure_swarm_and_runtime(conn, Path(db_path), swarm_id)
            name = str(name).strip()
            brief = str(brief)
            model = str(kwargs.get("model") or "default")
            provider = str(kwargs.get("provider") or "custom")
            reasoning_effort = kwargs.get("reasoning_effort")
            workspace_mode = str(kwargs.get("workspace", kwargs.get("workspace_mode", "inherit")))
            tasks = kwargs.get("tasks")
            n = int(kwargs.get("n", 1) or 1)

            def _resolve_ws_path(w_name: str) -> str:
                if workspace_mode in ("share", "branch") and (Path(os.getcwd()) / ".git").exists():
                    try:
                        return str(create_worker_workspace(os.getcwd(), swarm_id, w_name, mode=workspace_mode))
                    except Exception:
                        return ""
                return ""

            if isinstance(tasks, (list, tuple)) and len(tasks) > 0:
                for idx, task_slice in enumerate(tasks, start=1):
                    worker_name = f"{name}-{idx}"
                    worker_brief = f"{brief}\nTask: {task_slice}"
                    add_member(
                        conn,
                        swarm_id,
                        name=worker_name,
                        role="worker",
                        pool_name=name,
                        brief=worker_brief,
                        model=model,
                        provider=provider,
                        reasoning_effort=reasoning_effort,
                        workspace_mode=workspace_mode,
                        workspace_path=_resolve_ws_path(worker_name),
                    )
                return f"Worker pool '{name}' ({len(tasks)} workers) added successfully."

            if n > 1:
                for idx in range(1, n + 1):
                    worker_name = f"{name}-{idx}"
                    add_member(
                        conn,
                        swarm_id,
                        name=worker_name,
                        role="worker",
                        pool_name=name,
                        brief=brief,
                        model=model,
                        provider=provider,
                        reasoning_effort=reasoning_effort,
                        workspace_mode=workspace_mode,
                        workspace_path=_resolve_ws_path(worker_name),
                    )
                return f"Worker pool '{name}' ({n} workers) added successfully."

            add_member(
                conn,
                swarm_id,
                name=name,
                role="worker",
                pool_name=kwargs.get("pool_name"),
                brief=brief,
                model=model,
                provider=provider,
                reasoning_effort=reasoning_effort,
                workspace_mode=workspace_mode,
                workspace_path=_resolve_ws_path(name),
            )
            return f"Worker '{name}' added successfully."

        elif action == "model":
            name = kwargs.get("name")
            new_model = kwargs.get("model")
            if not name or not new_model:
                raise ValueError("name and model parameters are required")
            member = get_member(conn, swarm_id, str(name))
            if not member:
                return f"Error: Member '{name}' not found."
            with write_txn(conn):
                conn.execute(
                    "UPDATE swarm_members SET model = ? WHERE swarm_id = ? AND name = ?",
                    (str(new_model), swarm_id, str(name)),
                )
            return f"Model for '{name}' switched to {new_model}"

        elif action == "kill":
            name = kwargs.get("name")
            if not name or not str(name).strip():
                raise ValueError("name is required for kill")
            name = str(name)
            if (
                _CURRENT_RUNTIME is not None
                and _CURRENT_DB_PATH is not None
                and Path(db_path) == _CURRENT_DB_PATH
                and swarm_id == _CURRENT_SWARM_ID
            ):
                _CURRENT_RUNTIME.kill_worker(name)
            else:
                member = get_member(conn, swarm_id, name)
                if member and member.get("state") == "killed":
                    return f"Worker '{name}' terminated."
                update_member_state(conn, swarm_id, name, "killed")
            return f"Worker '{name}' terminated."

        else:
            raise ValueError(f"Unknown swarm action: {action}")
    finally:
        conn.close()


def post_message(text: Any = "", mentions: Optional[List[str]] = None, **kwargs: Any) -> str:
    """Post message to board and wake mentioned workers."""
    if isinstance(text, dict):
        merged = dict(text)
        merged.update(kwargs)
        text = merged.pop("text", "")
        if mentions is None:
            mentions = merged.pop("mentions", None)
        kwargs = merged
    if text is None or text == "":
        raise ValueError("Message text cannot be empty")

    ctx = _extract_context(kwargs)
    agent = kwargs.get("agent") or kwargs.get("parent_agent")
    author = (
        kwargs.get("author")
        or ctx.get("worker_name")
        or ctx.get("author")
        or getattr(agent, "swarm_member_name", None)
        or "user"
    )
    db_path, swarm_id = _resolve_db_and_swarm(kwargs)

    if not db_path:
        raise RuntimeError("Database connection unavailable")

    conn = init_swarm_db(db_path)
    try:
        if get_swarm(conn, swarm_id) is None:
            create_swarm(conn, swarm_id, "Active Swarm", "coordinator", os.getcwd())
        members = get_members(conn, swarm_id)
        resolved_mentions = sorted(parse_mentions(text, members))
        if mentions:
            for m in mentions:
                if m and m not in resolved_mentions:
                    resolved_mentions.append(m)

        hop_count = int(kwargs.get("hop_count", 0) or 0)
        seq = append_message(conn, swarm_id, str(author), resolved_mentions, text, hop_count=hop_count)
    finally:
        conn.close()

    parent_agent = kwargs.get("parent_agent")
    is_live_dispatch = parent_agent is not None or bool(kwargs.get("task_id") or kwargs.get("conversation_id"))
    if is_live_dispatch and _CURRENT_RUNTIME is None:
        conn_rt = init_swarm_db(db_path)
        try:
            _ensure_swarm_and_runtime(conn_rt, Path(db_path), swarm_id)
        finally:
            conn_rt.close()

    if (
        _CURRENT_RUNTIME is not None
        and _CURRENT_DB_PATH is not None
        and Path(db_path) == _CURRENT_DB_PATH
        and swarm_id == _CURRENT_SWARM_ID
    ):
        res = _CURRENT_RUNTIME.handle_message_posted(seq, str(author), resolved_mentions, text, hop_count=hop_count)
        is_worker_caller = bool(getattr(parent_agent, "swarm_member_name", None)) or str(author) not in (
            "user",
            "coordinator",
            "swarm_coordinator",
        )
        if is_live_dispatch and not is_worker_caller:
            for woken_name in res.get("woken", []):
                _spawn_live_worker_turn(
                    _CURRENT_RUNTIME,
                    Path(db_path),
                    swarm_id,
                    woken_name,
                    str(text),
                    parent_agent,
                )

    return f"Message posted with seq {seq}"


def read_board(since: Any = 0, limit: int = 50, **kwargs: Any) -> str:
    """Read recent messages from public board."""
    if isinstance(since, dict):
        merged = dict(since)
        merged.update(kwargs)
        since = merged.pop("since", 0)
        limit = merged.pop("limit", limit)
        kwargs = merged
    try:
        since_val = max(0, int(since))
    except (ValueError, TypeError):
        since_val = 0

    try:
        limit_val = max(0, min(int(limit), 500))
    except (ValueError, TypeError):
        limit_val = 50

    db_path, swarm_id = _resolve_db_and_swarm(kwargs)
    if limit_val == 0 or not db_path:
        return "No messages found."

    conn = init_swarm_db(db_path)
    try:
        msgs = get_messages(conn, swarm_id, since_seq=since_val, limit=limit_val)
        if not msgs:
            return "No messages found."
        lines = []
        for m in msgs:
            mentions_list = m.get("mentions") or []
            mentions_str = " ".join(str(x) for x in mentions_list) if mentions_list else ""
            lines.append(f"[{m['seq']}] <{m['author']}>: {m['text']} {mentions_str}".strip())
        return "\n".join(lines)
    finally:
        conn.close()


def swarm_wait(since: Any = 0, timeout: float = 30.0, **kwargs: Any) -> str:
    """In-turn blocking wait tool for new board messages."""
    if isinstance(since, dict):
        merged = dict(since)
        merged.update(kwargs)
        since = merged.pop("since", 0)
        timeout = merged.pop("timeout", timeout)
        kwargs = merged
    try:
        timeout_val = max(0.0, float(timeout))
    except (ValueError, TypeError):
        timeout_val = 30.0

    try:
        since_val = max(0, int(since))
    except (ValueError, TypeError):
        since_val = 0

    if _CURRENT_RUNTIME is not None:
        msgs = _CURRENT_RUNTIME.wait_for_messages(since_seq=since_val, timeout=timeout_val)
        if msgs:
            lines = [f"[{m['seq']}] <{m['author']}>: {m['text']}" for m in msgs]
            return "\n".join(lines)
        return "Wait timed out. No new messages."

    # Fallback to direct DB read
    db_path, swarm_id = _resolve_db_and_swarm(kwargs)
    if not db_path:
        return "Wait timed out. No new messages."

    conn = init_swarm_db(db_path)
    try:
        msgs = get_messages(conn, swarm_id, since_seq=since_val, limit=50)
        if msgs:
            lines = [f"[{m['seq']}] <{m['author']}>: {m['text']}" for m in msgs]
            return "\n".join(lines)
        return "Wait timed out. No new messages."
    finally:
        conn.close()


register_swarm_toolsets()
