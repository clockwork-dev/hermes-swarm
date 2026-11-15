"""Swarm Runtime & Wakeup Engine.

Coordinates worker execution, lifecycle state transitions, mention resolution,
cyclic loop guards, zero-token dormant waiting, and completion_queue wakeups.
"""

from __future__ import annotations

import concurrent.futures
import queue
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .state import (
    add_member,
    append_message,
    get_member,
    get_members,
    get_messages,
    init_swarm_db,
    update_member_state,
)

# Robust mention regex with negative lookbehind to avoid email addresses
MENTION_REGEX = re.compile(r'(?<![\w.-])@([a-zA-Z0-9_-]+)')

# Coordinator/human author aliases that reset hop counter
COORDINATOR_ALIASES = frozenset({"user", "human", "coordinator", "swarm_coordinator", "system"})

# Maximum autonomous worker-to-worker hops before escalation
MAX_AUTONOMOUS_HOPS = 3


def parse_mentions(text: str, active_members: Optional[List[dict]] = None) -> Set[str]:
    """Parse mention tokens from text (@worker, @pool, @all, @coordinator).

    Parameters:
        text: Input string to search for mentions.
        active_members: Optional list of member records from SQLite to resolve pools and @all.

    Returns:
        Set of resolved tokens, including '@name', member names, and '@all'/'@coordinator'.
    """
    if not text or not text.strip():
        return set()

    tokens = set(MENTION_REGEX.findall(text))
    resolved: Set[str] = set()

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
            if active_members:
                for m in active_members:
                    if m.get("state") != "killed":
                        if m.get("name") == tok:
                            resolved.add(m["name"])
                        elif m.get("pool_name") == tok or m.get("name", "").startswith(f"{tok}-"):
                            resolved.add(m["name"])

    return resolved


def seed_worker_context(
    member_name: str,
    brief: str,
    parent_history: Optional[List[dict]] = None,
    board_messages: Optional[List[dict]] = None,
    fork: Optional[bool] = None,
) -> List[dict]:
    """Seed initial prompt context for a worker from assigned brief and board."""
    if fork is None:
        fork = bool(parent_history)

    context: List[dict] = []
    if parent_history and fork:
        context.extend([dict(m) for m in parent_history])

    prompt_lines = [
        f"You are swarm worker '{member_name}'.",
        f"Assigned Brief: {brief}",
        "",
        "Communication Protocol:",
        "- Use post_message(text, mentions) to post to the board and alert teammates.",
        "- Use read_board(since, limit) to read messages.",
        "- Use swarm_wait(since, timeout) to wait for new messages with zero token cost.",
    ]

    if board_messages:
        prompt_lines.append("")
        prompt_lines.append("Recent Board Messages:")
        for bm in board_messages:
            prompt_lines.append(f"[{bm.get('seq', '?')}] {bm.get('author', 'unknown')}: {bm.get('text', '')}")

    context.append({"role": "system", "content": "\n".join(prompt_lines)})
    return context


class SwarmRuntime:
    """Runtime engine managing workers, message wakeup bus, and mention loop guards."""

    def __init__(
        self,
        swarm_id: str,
        db_path: Path | str,
        coordinator_sid: str = "",
        max_concurrent: int = 4,
    ) -> None:
        self.swarm_id = str(swarm_id)
        self.db_path = Path(db_path)
        self.coordinator_sid = str(coordinator_sid)
        self.max_concurrent = max(1, int(max_concurrent))

        self.cond = threading.Condition()
        self.listeners: Dict[str, List[threading.Event]] = {}
        self.completion_queue: Optional[Any] = None

        self._lock = threading.RLock()
        self._running = True
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_concurrent,
            thread_name_prefix=f"swarm-{self.swarm_id}",
        )
        self._futures: Dict[str, concurrent.futures.Future] = {}
        self._stop_events: Dict[str, threading.Event] = {}
        self._pool_rr: Dict[str, int] = {}

    def register_listener(self, worker_name: str, event: threading.Event) -> None:
        """Register a threading.Event to be signaled when worker_name is woken."""
        with self._lock:
            self.listeners.setdefault(worker_name, []).append(event)

    def unregister_listener(self, worker_name: str, event: threading.Event) -> None:
        """Unregister a threading.Event listener."""
        with self._lock:
            if worker_name in self.listeners:
                try:
                    self.listeners[worker_name].remove(event)
                except ValueError:
                    pass

    def _is_coordinator_or_human(self, author: str) -> bool:
        """Check if author is coordinator, user, human, or system."""
        if not author:
            return False
        lowered = author.lower()
        if lowered in COORDINATOR_ALIASES:
            return True
        if self.coordinator_sid and author == self.coordinator_sid:
            return True
        return False

    def check_hop_count(
        self,
        hop_count: int,
        author: str,
        targets: List[str],
    ) -> Tuple[int, List[str], bool]:
        """Enforce 3-hop limit, returning (effective_hop, targets, is_suppressed)."""
        if self._is_coordinator_or_human(author):
            return 0, targets, False

        effective_hop = max(0, hop_count)
        if effective_hop >= MAX_AUTONOMOUS_HOPS:
            return effective_hop, ["@coordinator"], True

        return effective_hop, targets, False

    def resolve_mentions(self, mentions: List[str]) -> List[str]:
        """Resolve a list of mention strings into target member names."""
        conn = init_swarm_db(self.db_path)
        try:
            members = get_members(conn, self.swarm_id)
        finally:
            conn.close()

        joined_text = " ".join(mentions)
        resolved = parse_mentions(joined_text, members)
        return sorted(list(resolved))

    def select_pool_worker(self, pool_name: str) -> Optional[str]:
        """Select an available worker in a sharded pool, preferring waiting state or round-robin."""
        conn = init_swarm_db(self.db_path)
        try:
            members = get_members(conn, self.swarm_id)
        finally:
            conn.close()

        pool_members = [
            m for m in members
            if (m.get("pool_name") == pool_name or m.get("name", "").startswith(f"{pool_name}-"))
            and m.get("state") != "killed"
        ]
        if not pool_members:
            return None

        waiting_members = [m for m in pool_members if m.get("state") == "waiting"]
        if waiting_members:
            return waiting_members[0]["name"]

        with self._lock:
            rr_idx = self._pool_rr.get(pool_name, 0)
            selected = pool_members[rr_idx % len(pool_members)]["name"]
            self._pool_rr[pool_name] = rr_idx + 1
            return selected

    def start_worker(
        self,
        name: str,
        task_fn: Optional[Callable[..., Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[concurrent.futures.Future]:
        """Set worker state to working in DB and optionally submit turn to thread pool."""
        conn = init_swarm_db(self.db_path)
        try:
            update_member_state(conn, self.swarm_id, name, "working")
        finally:
            conn.close()

        if task_fn is not None and self._running:
            with self._lock:
                stop_ev = threading.Event()
                self._stop_events[name] = stop_ev
                fut = self._executor.submit(task_fn, *args, **kwargs)
                self._futures[name] = fut
                return fut
        return None

    def stop_worker(self, name: str) -> None:
        """Transition worker state to stopped and signal cancellation."""
        with self._lock:
            if name in self._stop_events:
                self._stop_events[name].set()

        conn = init_swarm_db(self.db_path)
        try:
            update_member_state(conn, self.swarm_id, name, "stopped")
        finally:
            conn.close()

    def kill_worker(self, name: str, land: bool = True) -> dict:
        """Transition worker to killed and deallocate resources."""
        with self._lock:
            if name in self._stop_events:
                self._stop_events[name].set()
            if name in self._futures:
                self._futures[name].cancel()

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

    def handle_message_posted(
        self,
        seq: int,
        author: str,
        mentions: Any,
        text: str,
        hop_count: int = 0,
    ) -> dict:
        """Handle posted message: notify condition variable, evaluate loop guard, trigger wakeups."""
        with self.cond:
            self.cond.notify_all()

        conn = init_swarm_db(self.db_path)
        try:
            members = get_members(conn, self.swarm_id)
        finally:
            conn.close()

        resolved_mentions = parse_mentions(text, members)
        if isinstance(mentions, (list, set, tuple)):
            for m in mentions:
                resolved_mentions.add(m if m.startswith("@") else f"@{m}")
                bare = m[1:] if m.startswith("@") else m
                for mem in members:
                    if mem.get("state") != "killed":
                        if mem.get("name") == bare:
                            resolved_mentions.add(mem["name"])
                        elif mem.get("pool_name") == bare or mem.get("name", "").startswith(f"{bare}-"):
                            resolved_mentions.add(mem["name"])

        is_coord = self._is_coordinator_or_human(author)
        effective_hop = 0 if is_coord else max(0, hop_count)

        # Cyclic mention loop guard: cap at 3 hops
        if not is_coord and effective_hop >= MAX_AUTONOMOUS_HOPS:
            alert_text = f"Mention loop cap reached ({effective_hop} hops). Escalating to coordinator."
            conn = init_swarm_db(self.db_path)
            try:
                append_message(conn, self.swarm_id, "system", ["@coordinator"], alert_text, hop_count=0)
            finally:
                conn.close()
            with self.cond:
                self.cond.notify_all()
            return {"suppressed": True, "woken": [], "alert_posted": True}

        woken: List[str] = []
        is_all = "@all" in resolved_mentions

        for m in members:
            name = m["name"]
            state = m["state"]
            if name == author:
                continue
            if state == "killed":
                continue

            target_matched = (f"@{name}" in resolved_mentions) or (name in resolved_mentions)
            if is_all and state != "stopped":
                target_matched = True

            if target_matched:
                if state == "stopped" and (f"@{name}" in resolved_mentions or name in resolved_mentions):
                    conn = init_swarm_db(self.db_path)
                    try:
                        update_member_state(conn, self.swarm_id, name, "working")
                    finally:
                        conn.close()

                woken.append(name)

                with self._lock:
                    for ev in self.listeners.get(name, []):
                        ev.set()

                if self.completion_queue is not None:
                    self.completion_queue.put({
                        "type": "swarm_wakeup",
                        "swarm_id": self.swarm_id,
                        "worker_name": name,
                        "session_key": m.get("session_id", ""),
                        "seq": seq,
                        "author": author,
                        "text": text,
                    })

        return {"suppressed": False, "woken": woken, "alert_posted": False}

    def wait_for_messages(self, since_seq: int, timeout: float = 30.0) -> List[dict]:
        """Block on condition variable until seq > since_seq is available or timeout expires."""
        deadline = time.monotonic() + timeout
        conn = init_swarm_db(self.db_path)
        try:
            with self.cond:
                msgs = get_messages(conn, self.swarm_id, since_seq=since_seq, limit=50)
                if msgs:
                    return msgs

                remaining = deadline - time.monotonic()
                while remaining > 0 and self._running:
                    self.cond.wait(timeout=min(remaining, 0.05))
                    msgs = get_messages(conn, self.swarm_id, since_seq=since_seq, limit=50)
                    if msgs:
                        return msgs
                    remaining = deadline - time.monotonic()
                return get_messages(conn, self.swarm_id, since_seq=since_seq, limit=50)
        finally:
            conn.close()

    def seed_worker_context(
        self,
        member_name: str,
        brief: str,
        parent_history: Optional[List[dict]] = None,
        board_messages: Optional[List[dict]] = None,
        fork: Optional[bool] = None,
    ) -> List[dict]:
        """Seed initial prompt context for a worker from assigned brief and board."""
        return seed_worker_context(
            member_name=member_name,
            brief=brief,
            parent_history=parent_history,
            board_messages=board_messages,
            fork=fork,
        )

    def shutdown(self) -> None:
        """Halt all workers and threads cleanly."""
        self._running = False
        with self.cond:
            self.cond.notify_all()
        with self._lock:
            for ev_list in self.listeners.values():
                for ev in ev_list:
                    ev.set()
            for stop_ev in self._stop_events.values():
                stop_ev.set()
        self._executor.shutdown(wait=False, cancel_futures=True)
