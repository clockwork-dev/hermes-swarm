"""Slash command handlers for /swarm and /teamwork."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Optional

from .state import (
    add_member,
    create_swarm,
    get_members,
    init_swarm_db,
    update_member_state,
)
from .tools import (
    get_tool_runtime,
    post_message as send_board_message,
    read_board,
    set_active_swarm,
)
from .worktree_manager import land_worker_workspace

SKILL_PATH = Path(__file__).resolve().parent / "skills" / "teamwork-coordinator" / "SKILL.md"
DEFAULT_COORDINATOR_PROMPT = (
    "Coordinator mode active (default rules): never edit source code directly; "
    "enforce disjoint file ownership; require an independent verifier."
)


def get_coordinator_system_prompt(
    session_info: Optional[dict] = None,
    skill_path: Optional[Path] = None,
) -> str:
    """Return the Teamwork Coordinator system prompt loaded from SKILL.md."""
    target_path = Path(skill_path) if skill_path is not None else SKILL_PATH
    try:
        if target_path.exists():
            text = target_path.read_text(encoding="utf-8").strip()
            if text:
                return text
    except Exception:
        pass
    return DEFAULT_COORDINATOR_PROMPT


def _resolve_tool_globals(ctx: dict) -> tuple[Optional[Path], str]:
    """Resolve (db_path, swarm_id) from session_context or tools runtime state."""
    db_path = ctx.get("db_path")
    swarm_id = ctx.get("swarm_id")
    try:
        _, curr_db, curr_swarm_id = get_tool_runtime()
        if not db_path:
            db_path = curr_db
        if not swarm_id:
            swarm_id = curr_swarm_id or "swarm-test-123"
    except Exception:
        if not swarm_id:
            swarm_id = "swarm-test-123"
    return (Path(db_path) if db_path else None), str(swarm_id or "swarm-test-123")


def handle_swarm_command(args: Any, session_context: Optional[dict] = None, **kwargs: Any) -> str:
    """Handle /swarm slash command and subcommands."""
    if isinstance(args, str):
        args_list = args.strip().split()
    elif isinstance(args, list):
        args_list = [str(x) for x in args]
    else:
        args_list = []

    if not args_list:
        return (
            "Usage: /swarm <task> | /swarm status | /swarm board [N] | "
            "/swarm add <name> <brief> | /swarm say <@worker> <text> | "
            "/swarm land <worker> | /swarm kill <worker|all> | /swarm stop"
        )

    subcmd = args_list[0].lower()
    ctx = dict(session_context) if isinstance(session_context, dict) else {}
    for k, v in kwargs.items():
        if v is not None and k not in ctx:
            ctx[k] = v
    db_path, swarm_id = _resolve_tool_globals(ctx)

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
            rows = [
                f"{m.get('name', '?')} | {m.get('role', 'worker')} | "
                f"{m.get('state', '?')} | {m.get('model') or 'default'} | "
                f"{m.get('workspace_mode', 'inherit')}"
                for m in members
            ]
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
        brief_tokens: list[str] = []
        i = 2
        model = "default"
        worktree = "inherit"
        effort: Optional[str] = None
        while i < len(args_list):
            if args_list[i] == "--model" and i + 1 < len(args_list):
                model = args_list[i + 1]
                i += 2
            elif args_list[i] in ("--worktree", "--workspace") and i + 1 < len(args_list):
                worktree = args_list[i + 1]
                i += 2
            elif args_list[i] == "--effort" and i + 1 < len(args_list):
                effort = args_list[i + 1]
                i += 2
            elif args_list[i].startswith("--"):
                return f"Error: Unrecognized flag '{args_list[i]}'"
            else:
                brief_tokens.append(args_list[i])
                i += 1
        brief = " ".join(brief_tokens).strip()
        if not brief:
            return "Error: Worker brief is required."
        if not db_path:
            return "No active swarm session."
        conn = init_swarm_db(db_path)
        try:
            add_member(
                conn,
                swarm_id,
                name,
                "worker",
                brief=brief,
                model=model,
                reasoning_effort=effort,
                workspace_mode=worktree,
            )
            return f"Worker '{name}' added to swarm."
        finally:
            conn.close()

    elif subcmd == "say":
        if len(args_list) < 2:
            return "Usage: /swarm say <@target> <message text>"
        target = args_list[1]
        text = " ".join(args_list[2:]).strip()
        if not text:
            return "Error: Message text cannot be empty."
        full_text = f"{target} {text}" if target.startswith("@") else f"@{target} {text}"
        return send_board_message(
            full_text, author="user", db_path=db_path, swarm_id=swarm_id, task_id="slash-say"
        )

    elif subcmd == "stop":
        if not db_path:
            return "No active swarm to stop."
        conn = init_swarm_db(db_path)
        try:
            members = get_members(conn, swarm_id)
            for m in members:
                if m["state"] in ("working", "waiting"):
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
        if not db_path:
            return f"Worker '{target}' already terminated or not found."
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
        repo_root = Path(ctx.get("workspace_root") or os.getcwd())
        res = land_worker_workspace(repo_root, target)
        if res.get("status") == "conflict":
            return f"Conflict detected while landing '{target}'. Changes preserved in worktree."
        return f"Worker '{target}' workspace landed successfully."

    else:
        task_str = " ".join(args_list)
        title = task_str[:40] + ("..." if len(task_str) > 40 else "")
        new_swarm_id = f"swarm-{uuid.uuid4().hex[:8]}"
        if db_path:
            conn = init_swarm_db(db_path)
            try:
                create_swarm(
                    conn,
                    new_swarm_id,
                    title,
                    str(ctx.get("session_id") or "coordinator-1"),
                    str(ctx.get("workspace_root") or os.getcwd()),
                )
                set_active_swarm(new_swarm_id, db_path)
            finally:
                conn.close()
        return f"Swarm coordinator initialized for task: '{title}' [ID: {new_swarm_id}]"


def handle_teamwork_command(task: Any, session_context: Optional[dict] = None, **kwargs: Any) -> str:
    """Handle /teamwork slash command: configure session as Teamwork Coordinator."""
    ctx = dict(session_context) if isinstance(session_context, dict) else {}
    for k, v in kwargs.items():
        if v is not None and k not in ctx:
            ctx[k] = v
    workspace_root = Path(ctx.get("workspace_root") or os.getcwd())

    if not (workspace_root / ".git").exists():
        return "Warning: Teamwork coordinator mode requires a git repository. Please initialize git first."

    if isinstance(task, list):
        task_str = " ".join(str(x) for x in task).strip()
    else:
        task_str = str(task or "").strip()

    if not task_str:
        return "Usage: /teamwork <task description>"

    if isinstance(session_context, dict):
        session_context["role"] = "swarm_coordinator"
        session_context["teamwork_mode"] = True

    return (
        f"Teamwork Coordinator mode initialized for: '{task_str}'. "
        "Strict rules enforced: no direct code edits; disjoint file ownership; independent verifier."
    )
