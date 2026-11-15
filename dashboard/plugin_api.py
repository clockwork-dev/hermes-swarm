"""Read-only FastAPI dashboard endpoints for Hermes Swarm."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Query

from ..state import get_members, get_messages, init_swarm_db
from ..tools import get_tool_runtime


def _resolve_db_file(explicit_db_path: Optional[Path | str]) -> Path:
    """Resolve the active swarm SQLite database file."""
    if explicit_db_path is not None:
        return Path(explicit_db_path)
    try:
        _, curr, _ = get_tool_runtime()
        if curr is not None:
            return Path(curr)
    except Exception:
        pass
    try:
        from hermes_constants import get_hermes_home

        for candidate in ("swarm_state.db", "swarms.db"):
            p = Path(get_hermes_home()) / candidate
            if p.exists():
                return p
        return Path(get_hermes_home()) / "swarm_state.db"
    except Exception:
        return Path.home() / ".hermes" / "swarm_state.db"


def create_dashboard_router(db_path: Path | str | None = None) -> APIRouter:
    """Create FastAPI router exposing read-only live inspection endpoints."""
    api_router = APIRouter()

    @api_router.get("/status")
    def get_status():
        db_file = _resolve_db_file(db_path)
        if not db_file.exists():
            return {
                "status": "ok",
                "active": False,
                "swarm_id": None,
                "members_count": 0,
                "messages_count": 0,
            }
        conn = init_swarm_db(db_file)
        try:
            cur = conn.cursor()
            cur.execute("SELECT swarm_id, title FROM swarms ORDER BY created_at DESC, rowid DESC LIMIT 1")
            swarm_row = cur.fetchone()
            if not swarm_row:
                return {
                    "status": "ok",
                    "active": False,
                    "swarm_id": None,
                    "members_count": 0,
                    "messages_count": 0,
                }
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

    @api_router.get("/board")
    def get_board(since: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=500)):
        db_file = _resolve_db_file(db_path)
        if not db_file.exists():
            return {"status": "ok", "messages": []}
        conn = init_swarm_db(db_file)
        try:
            cur = conn.cursor()
            cur.execute("SELECT swarm_id FROM swarms ORDER BY created_at DESC, rowid DESC LIMIT 1")
            swarm_row = cur.fetchone()
            if not swarm_row:
                return {"status": "ok", "messages": []}
            msgs = get_messages(conn, swarm_row["swarm_id"], since_seq=since, limit=limit)
            return {"status": "ok", "messages": msgs}
        finally:
            conn.close()

    @api_router.get("/members")
    def get_members_list():
        db_file = _resolve_db_file(db_path)
        if not db_file.exists():
            return {"status": "ok", "members": []}
        conn = init_swarm_db(db_file)
        try:
            cur = conn.cursor()
            cur.execute("SELECT swarm_id FROM swarms ORDER BY created_at DESC, rowid DESC LIMIT 1")
            swarm_row = cur.fetchone()
            if not swarm_row:
                return {"status": "ok", "members": []}
            mems = get_members(conn, swarm_row["swarm_id"])
            return {"status": "ok", "members": mems}
        finally:
            conn.close()

    return api_router


router = create_dashboard_router()
