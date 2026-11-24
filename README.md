# SmartDialer — Fault-Tolerant Outbound Dialing Engine

A lightweight, concurrency-safe outbound SmartDialer prototype supporting **Progressive** and **Predictive** dialing modes, enforced by an independent **Safety Controller**, idempotent out-of-order webhook handling, multi-provider failover, and worker crash recovery.

---

## Project Structure

```text
smartdialer/
├── src/
│   ├── __init__.py
│   ├── models.py       # Agent & Call state machines, monotonic rank map, domain dataclasses
│   ├── store.py        # SQLite WAL state store with atomic CAS locks & event deduplication
│   ├── providers.py    # TelecomProvider interface + Mock Providers A (fast) & B (noisy/out-of-order)
│   ├── pacing.py       # Progressive (1:1) & Predictive statistical pacing engine
│   ├── safety.py       # Non-bypassable SafetyController (circuit breaker, abandon cap, progressive fallback)
│   ├── dialer.py       # Core orchestrator, CallAllocator, idempotent webhook handler, crash sweeper
│   └── simulator.py    # Multi-scenario simulation (20%/50%/70%/dynamic answer rates, outages, agent drops)
├── tests/
│   ├── test_smartdialer.py  # Unit & concurrency tests (race conditions, crashes, out-of-order events)
│   └── load_test.py         # Multi-worker concurrent load test verifying zero double-allocations
└── docs/
    └── ARCHITECTURE.md      # Architecture diagram, state machines, trade-offs, scaling plan & final answer
```

---

## Setup & Execution (Zero External Dependencies — Python 3.9+)

No third-party packages are required; the system runs strictly on the Python standard library (`sqlite3`, `threading`, `unittest`, `concurrent.futures`).

### 1. Navigate to the Project Directory
```bash
cd smartdialer
```

### 2. Run the Unit & Concurrency Test Suite
Verifies concurrent agent reservation races, progressive 1:1 invariants, duplicate/out-of-order webhook handling, worker crash recovery, provider failover, and non-bypassable safety boundaries:
```bash
python3 -m unittest discover -s tests -v
```

### 3. Run the Multi-Scenario Simulation (Scenarios A–E + Baseline)
Evaluates utilization, calls initiated/connected, abandonment rates, and SafetyController decisions across 20%, 50%, 70%, and dynamically shifting answer rates, provider outages, and sudden 40% agent drops:
```bash
python3 -m src.simulator
```

### 4. Run the Multi-Worker Concurrent Load Test
Spawns 8 concurrent worker threads competing over 200 agents and 1,000 borrowers to verify zero double-allocations and zero orphaned locks:
```bash
python3 -m tests.load_test
```

---

## High-Level Architecture

```text
+-------------------+       +------------------------+       +---------------------+
|  Campaign Queue   | ----> |     Pacing Engine      | ----> |  Safety Controller  |
| (Pending Borrowers|       | (Progressive/Predictive|       | (Hard Guardrails &  |
|  & Live Metrics)  |       |  Statistical Estimator)|       |  Circuit Breaker)   |
+-------------------+       +------------------------+       +---------------------+
                                                                        |
                                                               (Approved Quota)
                                                                        v
+-------------------+       +------------------------+       +---------------------+
|  Provider A & B   | <---- |    Telecom Interface   | <---- |   Call Allocator    |
|  (Mock Providers) |       |  (Health-Based Router) |       | (Atomic CAS Locks)  |
+-------------------+       +------------------------+       +---------------------+
         |                                                              |
         +------------------> [ Idempotent Webhook Handler ] -----------+
                                           |
                                           v
                            +-----------------------------+
                            |   StateStore (SQLite WAL)   |
                            | - Agents (Versioned CAS)    |
                            | - Borrowers (Atomic Claim)  |
                            | - Calls (Monotonic Rank)    |
                            | - Processed Events (Dedup)  |
                            +-----------------------------+
                                           ^
                                           |
                            +-----------------------------+
                            |  Background Crash Sweeper   |
                            | (Reclaims Stale Locks/Calls)|
                            +-----------------------------+
```

---

## Key Design Highlights

1. **Zero Double-Allocation:** `StateStore.atomic_reserve_agent()` uses SQLite `BEGIN IMMEDIATE` + optimistic version Compare-And-Swap (`WHERE state='AVAILABLE' AND version=?`) so concurrent workers never claim the same agent or borrower.
2. **Non-Bypassable Safety Layer:** `PacingEngine` only emits a `PacingProposal`. `SafetyController` independently enforces the 3% rolling abandonment threshold, provider outage circuit breaker, live agent availability checks, and automatic fallback to `PROGRESSIVE` mode. The allocator re-checks safety immediately before every call and atomically reserves one real agent before dialing in both modes. Predictive pacing can propose a batch, but it cannot over-dial beyond currently available agents.
3. **Agent-backed Predictive Safety:** Predictive pacing adjusts the requested batch using observed answer rate, talk time, setup time, and provider health. The safety layer clamps that batch to available agents, and each individual allocation performs a fresh safety check plus an atomic agent reservation. This intentionally sacrifices speculative over-dialing to preserve deterministic agent coverage; it avoids relying on an unimplemented call-parking/virtual-reservation mechanism.
4. **Idempotent & Out-of-Order Event Handling:** `processed_events` drops duplicate webhooks in $O(1)$, while `CALL_STATE_RANK` ensures late or out-of-order events (e.g., `COMPLETED -> ANSWERED -> RINGING`) never regress terminal states.
5. **Crash & Stale Lock Recovery:** `recover_stale_resources()` sweeps orphaned `RESERVED`/`DIALING` agents and timed-out calls if a worker crashes mid-setup, returning agents to `AVAILABLE` and requeuing borrowers.

---

## Full Design Documentation & Final Question

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for:
- Detailed **Agent & Call State Machines** and concurrency invariants
- **Architecture Decision Record** (choices, trade-offs, and failure modes)
- **Scaling Bottleneck Analysis** (`100 -> 1,000 -> 10,000` agents)
- **Answer to the Final Question** (achieving predictive utilization with deterministic progressive safety)
