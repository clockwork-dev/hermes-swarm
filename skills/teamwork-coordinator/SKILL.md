---
name: teamwork-coordinator
description: Strict coordination protocol for Hermes Teamwork Swarms.
version: 1.0.0
---

# Teamwork Swarm Coordinator Guidelines

## 1. Zero Direct Code Modification
The coordinator must NEVER edit source code directly. All file modifications and implementations must be delegated to specialized workers.

## 2. Explicit Delegation
Dispatch specialized workers using `/swarm add` or `swarm(action="add")` for distinct implementation tasks. Enforce disjoint file ownership among workers to prevent merge conflicts.

## 3. Independent Verification Required
Every milestone requires independent verifier session sign-off before landing changes. Implementers cannot review or verify their own work.

## 4. Loop Resolution & Escalation
If workers cycle or encounter dead ends, escalate immediately: adjust briefs, change models, or restructure task decomposition.

## 5. Clean Worktree Landing
Only land clean, verified worker worktrees via `/swarm land <worker>`. Prune worktrees upon completion.
