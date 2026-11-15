# hermes-swarm — Native `/swarm` & `/teamwork` Multi-Agent Plugin for Hermes Agent

Production-grade in-session multi-agent swarm orchestration and autonomous teamwork coordination plugin for **Hermes Agent** (`>=0.21.4`, verified on `v0.23.1`).

---

## Key Features

- **Shared SQLite WAL Message Board (`state.py`)**: Concurrent-safe persistence (`swarms`, `swarm_members`, `swarm_messages`) with strictly monotonic message sequence numbers (`#seq`).
- **Event-Driven `@mention` Wakeup Bus (`runtime.py`)**:
  - Zero-token dormant `waiting` state for idle workers.
  - Supports `@worker`, `@pool` (sharded worker pools `name-1..k`), `@all`, and `@coordinator`.
  - 3-hop autonomous worker-to-worker loop guard with automatic `@coordinator` escalation.
  - Context isolation: each worker starts with a clean context seeded from its `brief` and the shared board (or optional parent history fork).
- **Git Worktree Isolation & Safe Landing (`worktree_manager.py`)**:
  - `inherit`: Read-only/shared workspace root (for `researcher` and `verifier` roles).
  - `share`: Detached Git worktree (`git worktree add --detach`) sharing `.git` object store without duplicating history.
  - `branch`: Dedicated branch worktree (`swarm/<swarm_id>/<worker>`).
  - Safe `land` operation with automatic conflict detection, abort-on-conflict preservation, and per-repo thread locking.
  - Full compatibility with `antigravity-oauth` parallel `oneshot` worker execution.
- **Model Tools (`tools.py`)**:
  - `swarm(action="status|models|add|model|kill|title", ...)`
  - `post_message(text, mentions)`
  - `read_board(since, limit)`
  - `swarm_wait(since, timeout)`
- **Slash Commands & Teamwork Skill (`slash_handlers.py`, `skills/teamwork-coordinator/SKILL.md`)**:
  - `/swarm <task>` — Initialize swarm coordinator for a task.
  - `/swarm status` / `/swarm board [N]` — Instant zero-LLM inspection of workers and board messages.
  - `/swarm add <name> [--model M] [--worktree share|branch] <brief>` — Manually add a worker.
  - `/swarm say <@target> <text>` — Post a human message to the board and wake mentioned agents.
  - `/swarm land <name>` / `/swarm kill <name|all>` / `/swarm stop` — Merge worker changes or stop workers.
  - `/teamwork <task>` — Strict **Teamwork Coordinator** protocol (*Research → Disjoint File Ownership Plan → Parallel Worktree Execution → Independent Verifier Loop*).
- **Read-Only Web Dashboard API (`dashboard/plugin_api.py`)**:
  - `GET /api/plugins/swarm/status`
  - `GET /api/plugins/swarm/board`
  - `GET /api/plugins/swarm/members`

---

## Quick Installation (Production Hermes Server)

Install directly via the Hermes CLI (no manual core file edits required):

```bash
# 1. Install plugin from GitHub
hermes plugins install https://github.com/clockwork-dev/hermes-swarm.git

# 2. Enable plugin
hermes plugins enable swarm

# 3. Restart gateway (if running Hermes Gateway service)
hermes gateway restart
```

Verify installation:

```bash
hermes plugins list
hermes plugins validate ~/.hermes/plugins/swarm
```

---

## Running Tests

```bash
pytest tests -v
```
