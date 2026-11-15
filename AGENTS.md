# AGENTS.md — Architectural Guidelines for `hermes-swarm`

## 1. Purpose

`hermes-swarm` is a standalone multi-agent coordination plugin (`kind: standalone`) for **Hermes Agent** (`>=0.21.4`). It provides native `/swarm` and `/teamwork` capabilities on top of a SQLite WAL message board, event-driven `@mention` wakeups, and Git worktree isolation.

## 2. Core Invariants

1. **Zero Secret Exfiltration**: Never store, log, or echo API keys, OAuth tokens, or credentials in `swarm_state.db` or board messages.
2. **Standalone + Bundled Compatibility**: All internal imports must work both as relative package imports (`hermes_plugins.swarm` when installed in `~/.hermes/plugins/swarm`) and via `plugins.swarm.*`.
3. **Zero-Token Dormant Waiting**: Sleeping workers (`waiting` state) must never busy-poll an LLM; they wake only on `SwarmRuntime` condition variables / `completion_queue` events triggered by `@mentions`.
4. **Disjoint Worktree Safety**: Parallel code-writing workers must use `share` or `branch` worktree modes, serialized per repository via `_repo_lock(repo_root)` to avoid `.git/config.lock` races on Windows and POSIX.
5. **Validation & Testing**:
   - `pytest tests -v` (315 tests across unit, pairwise, and E2E tiers)
   - `hermes plugins validate /path/to/hermes-swarm` (0 warnings, 0 errors, security scan: safe)
