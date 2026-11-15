"""Native Swarm & Teamwork multi-agent coordination plugin."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PLUGIN_DIR = Path(__file__).resolve().parent

_DEFAULT_TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    "swarm": {
        "name": "swarm",
        "description": "Coordinator administrative management tool for the agent swarm.",
        "parameters": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["status", "models", "add", "model", "kill", "title"],
                },
                "name": {"type": "string"},
                "brief": {"type": "string"},
                "model": {"type": "string"},
                "workspace": {"type": "string", "enum": ["inherit", "share", "branch"]},
                "title": {"type": "string"},
            },
            "required": ["action"],
        },
    },
    "post_message": {
        "name": "post_message",
        "description": "Post a message to the shared swarm message board and wake @mentioned workers.",
        "parameters": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "mentions": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["text"],
        },
    },
    "read_board": {
        "name": "read_board",
        "description": "Read recent messages from the shared swarm message board.",
        "parameters": {
            "type": "object",
            "properties": {
                "since": {"type": "integer", "default": 0},
                "limit": {"type": "integer", "default": 50},
            },
        },
    },
    "swarm_wait": {
        "name": "swarm_wait",
        "description": "Block in-turn until a new message arrives on the swarm board or timeout expires.",
        "parameters": {
            "type": "object",
            "properties": {
                "since": {"type": "integer", "default": 0},
                "timeout": {"type": "number", "default": 30.0},
            },
        },
    },
}


def register(ctx: Any) -> None:
    """Plugin entrypoint conforming to both test MockContext and Hermes PluginContext."""
    from . import tools as tools_mod
    from .slash_handlers import (
        get_coordinator_system_prompt,
        handle_swarm_command,
        handle_teamwork_command,
    )

    is_real_ctx = ctx.__class__.__name__ == "PluginContext"
    if is_real_ctx:
        try:
            from hermes_constants import get_hermes_home
            tools_mod.set_default_db_path(Path(get_hermes_home()) / "swarm_state.db")
        except Exception:
            pass

    tool_schemas = getattr(tools_mod, "TOOL_SCHEMAS", _DEFAULT_TOOL_SCHEMAS)
    tool_map = [
        ("swarm", tools_mod.swarm),
        ("post_message", tools_mod.post_message),
        ("read_board", tools_mod.read_board),
        ("swarm_wait", tools_mod.swarm_wait),
    ]

    if hasattr(ctx, "register_tool"):
        for tool_name, tool_fn in tool_map:
            raw_schema = tool_schemas.get(tool_name, _DEFAULT_TOOL_SCHEMAS.get(tool_name, {}))
            inner_schema = (
                raw_schema.get("function", raw_schema)
                if isinstance(raw_schema, dict)
                else _DEFAULT_TOOL_SCHEMAS.get(tool_name, {})
            )
            try:
                ctx.register_tool(tool_name, tool_fn)
            except TypeError:
                ctx.register_tool(
                    name=tool_name,
                    toolset="swarm_coordinator" if tool_name == "swarm" else "swarm_worker",
                    schema=inner_schema,
                    handler=tool_fn,
                    description=tool_fn.__doc__ or "",
                )

    if hasattr(ctx, "register_slash_command"):
        try:
            ctx.register_slash_command("swarm", handle_swarm_command)
            ctx.register_slash_command("teamwork", handle_teamwork_command)
        except TypeError:
            pass

    if hasattr(ctx, "register_command"):
        def _dispatch_coordinator_turn(coord_prompt: str, cmd_label: str, init_res: str, kw: dict[str, Any]) -> None:
            manager = getattr(ctx, "_manager", None)
            session_key = str(kw.get("session_key") or kw.get("session_id") or "")
            session_id = str(kw.get("session_id") or "")
            source = kw.get("source")
            if manager is not None and getattr(manager, "has_tui_message_injector", False) and session_key:
                try:
                    if manager.inject_tui_message(
                        session_key=session_key,
                        content=coord_prompt,
                        plugin_id=getattr(ctx, "plugin_id", "swarm"),
                    ):
                        return
                except Exception:
                    pass
            if manager is not None and getattr(manager, "has_gateway_message_injector", False):
                try:
                    if session_id and session_key:
                        if manager.inject_gateway_message(
                            session_key=session_key,
                            plugin_home=getattr(manager, "home_path", None),
                            content=coord_prompt,
                            plugin_id=getattr(ctx, "plugin_id", "swarm"),
                        ):
                            return
                    if source is not None and callable(getattr(source, "to_dict", None)):
                        if manager.inject_gateway_message(
                            origin=dict(source.to_dict()),
                            plugin_home=getattr(manager, "home_path", None),
                            content=coord_prompt,
                            plugin_id=getattr(ctx, "plugin_id", "swarm"),
                        ):
                            return
                    if session_key:
                        if manager.inject_gateway_message(
                            session_key=session_key,
                            plugin_home=getattr(manager, "home_path", None),
                            content=coord_prompt,
                            plugin_id=getattr(ctx, "plugin_id", "swarm"),
                        ):
                            return
                except Exception:
                    pass
            cli_ref = getattr(manager, "_cli_ref", None)
            if cli_ref is not None:
                if callable(getattr(cli_ref, "_queue_prompt_turn", None)) and getattr(cli_ref, "agent", None) is not None:
                    cli_ref._queue_prompt_turn(coord_prompt, cmd_label, str(init_res))
                elif hasattr(cli_ref, "_pending_input"):
                    cli_ref._pending_input.put(coord_prompt)

        def _plugin_swarm_cmd(raw_args: str = "", **kw: Any) -> str:
            res = handle_swarm_command(raw_args, **kw)
            tokens = str(raw_args or "").strip().split()
            if tokens and tokens[0].lower() not in {"status", "board", "add", "say", "land", "kill", "stop"}:
                task_str = " ".join(tokens)
                coord_prompt = (
                    f"{get_coordinator_system_prompt()}\n\n"
                    f"Active Swarm Task: {task_str}\n"
                    "Use `swarm`, `post_message`, `read_board`, and `swarm_wait` tools "
                    "(if deferred behind tool_search, invoke them via `tool_call(tool=..., arguments=...)`) "
                    "to coordinate workers."
                )
                _dispatch_coordinator_turn(coord_prompt, "/swarm", str(res), kw)
            return res

        def _plugin_teamwork_cmd(raw_args: str = "", **kw: Any) -> str:
            res = handle_teamwork_command(raw_args, **kw)
            task_str = str(raw_args or "").strip()
            if task_str and not str(res).startswith("Warning:") and not str(res).startswith("Usage:"):
                coord_prompt = (
                    f"{get_coordinator_system_prompt()}\n\n"
                    f"Teamwork Task: {task_str}\n"
                    "Use `swarm`, `post_message`, `read_board`, and `swarm_wait` tools "
                    "(if deferred behind tool_search, invoke them via `tool_call(tool=..., arguments=...)`) "
                    "to coordinate workers."
                )
                _dispatch_coordinator_turn(coord_prompt, "/teamwork", str(res), kw)
            return res

        try:
            ctx.register_command(
                "swarm",
                _plugin_swarm_cmd if is_real_ctx else handle_swarm_command,
                description="Manage multi-agent swarm coordination",
                args_hint="[status|board|add|say|land|kill|stop|<task>]",
            )
            ctx.register_command(
                "teamwork",
                _plugin_teamwork_cmd if is_real_ctx else handle_teamwork_command,
                description="Initialize strict Teamwork Coordinator mode",
                args_hint="<task>",
            )
        except Exception:
            pass

    if hasattr(ctx, "register_skill"):
        skill_md = _PLUGIN_DIR / "skills" / "teamwork-coordinator" / "SKILL.md"
        if skill_md.exists():
            try:
                ctx.register_skill(
                    "teamwork-coordinator",
                    skill_md,
                    description="Coordinate multi-agent teamwork with disjoint file ownership and independent verification.",
                )
            except Exception:
                pass

    if hasattr(ctx, "register_system_prompt_section"):
        try:
            SWARM_SYSTEM_PROMPT_GUIDANCE = (
                "Swarm & Teamwork tools (`swarm`, `post_message`, `read_board`, `swarm_wait`) "
                "coordinate parallel workers via a shared SQLite WAL message board."
            )
            ctx.register_system_prompt_section("swarm_guidance", SWARM_SYSTEM_PROMPT_GUIDANCE)
        except Exception:
            pass
