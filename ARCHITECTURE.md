# Architecture Decision Record & Design Document

## 1. System Architecture Diagram

```
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

## 2. State Machines

### Agent State Machine
```
OFFLINE <---> AVAILABLE ---> RESERVED ---> DIALING ---> CONNECTED ---> WRAP_UP ---> AVAILABLE
                  ^             |             |
                  |             +-------------+ (Call Failed / Setup Abort)
                  +--- PAUSED
```
- **Atomicity Rule:** Moving an agent from `AVAILABLE -> RESERVED` uses an atomic Compare-And-Swap (`UPDATE agents SET state='RESERVED', version=version+1 WHERE agent_id=? AND state='AVAILABLE' AND version=?`). Even if 100 workers read the same `AVAILABLE` agent simultaneously, SQLite's `BEGIN IMMEDIATE` + row-version check guarantees strictly **one** worker gets `rowcount == 1`.

### Call State Machine (Monotonic Rank Guard)
```
QUEUED (0) -> RESERVED (1) -> INITIATED (2) -> RINGING (3) -> ANSWERED (4) -> CONNECTED (5) -> COMPLETED (6)
                                                                 |
                                                                 +-> CANCELLED (6) [Cancelled / provider hang-up; abandoned flag only if agent coverage is lost]
                                                                 +-> FAILED (6)
```
- **Out-of-Order & Duplicate Protection:**
  1. Every webhook `event_id` is inserted into `processed_events` (`PRIMARY KEY`). Exact duplicate deliveries are dropped in O(1).
  2. Each `CallState` has a numeric rank (`QUEUED=0` ... `COMPLETED/FAILED/CANCELLED=6`). `advance_call_state` rejects any transition where `rank(target) < rank(current)` or `current` is already terminal. Thus `COMPLETED -> ANSWERED -> RINGING` leaves the call cleanly in `COMPLETED`.

---

## 3. Architectural Choices & Trade-offs

| Decision | Choice Made | Problem Solved | What It Makes Harder |
| :--- | :--- | :--- | :--- |
| **State Storage** | SQLite in WAL mode with Optimistic Concurrency Control (`version` column) | Zero external infra setup (`pip install` free); provides true ACID transactions and atomic CAS locks across multi-threaded/multi-process workers. | Single-node write throughput ceiling (~5k-10k writes/sec) compared to sharded PostgreSQL or Redis Lua scripts. |
| **Safety Boundary** | Separate `SafetyController` gates the pacing proposal and is re-evaluated at the allocator boundary for every call | Prevents stale batch approvals and direct allocator calls from bypassing provider, abandonment, or live-agent checks. Each call reserves one concrete agent before dialing in both modes. | Predictive pacing cannot speculate beyond available agents; utilization is intentionally more conservative in exchange for deterministic agent coverage. |
| **Provider Abstraction** | `TelecomProvider` ABC with rolling error/latency telemetry | Isolates vendor timeouts, duplicate webhooks, and out-of-order events; enables automatic failover from degraded Provider A to Provider B. | Requires maintaining a unified internal `ProviderEvent` schema across heterogeneous telecom carriers. |
| **Crash Recovery** | Lease/timestamp-based background sweeper (`recover_stale_resources`) | Reclaims agents stuck in `RESERVED`/`DIALING` and requeues borrowers if a worker crashes mid-setup. | Adds a small recovery delay equal to `stale_timeout_sec` before orphaned resources return to the pool. |

---

## 4. Scaling Analysis: 100 -> 1,000 -> 10,000 Agents

### What Breaks First and Why?
1. **At ~1,000 Agents (Database Lock Contention on `AVAILABLE` Agent Selection):**
   - Multiple dialer workers running `SELECT ... WHERE state='AVAILABLE' LIMIT 1` followed by `UPDATE` contend on the same hot rows/index pages. In SQLite, `BEGIN IMMEDIATE` serializes all write transactions behind a single writer lock, causing p99 allocation latency to spike.
2. **At ~10,000 Agents (Webhook & Pacing Loop Bottleneck):**
   - At 10,000 agents (~300–500 call state transitions/sec), a single centralized pacing loop scanning all active calls/agents every tick becomes CPU- and I/O-bound, and synchronous webhook processing can back up telecom provider HTTP connections.

### How to Fix It
1. **Replace SQLite with PostgreSQL (`FOR UPDATE SKIP LOCKED`) or Redis Atomic Queues:**
   - Maintaining an `available_agents` FIFO queue (`LPOP` in Redis or `SELECT ... FOR UPDATE SKIP LOCKED` in Postgres) eliminates lock contention because concurrent workers immediately claim distinct agents without blocking on the same row.
2. **Partition by Campaign / Tenant Shard:**
   - Shard pacing engines and state stores by `campaign_id` (consistent hashing across worker nodes). Ten campaigns of 1,000 agents each run completely independently with zero cross-shard lock contention.
3. **Decouple Webhook Ingestion via Durable Queue:**
   - Acknowledge provider webhooks immediately (`202 Accepted`) after pushing raw events onto an idempotent partitioned stream (partitioned by `provider_call_id`), ensuring strict per-call ordering while scaling consumers horizontally.

---

## 5. Final Question: Predictive Utilization with Progressive-Level Safety

This prototype chooses a conservative hybrid: use predictive statistics to decide the requested batch size, but retain the progressive mode's deterministic allocation invariant.

1. **Predictive pacing, not predictive authority:** The pacing engine estimates demand from recent answer rate, average talk time, call setup time, and provider health. It can request a batch, but it cannot place calls or grant itself additional capacity.
2. **Independent safety approval:** The Safety Controller clamps the batch to the current number of AVAILABLE agents, rejects new calls when no agents are available or the provider circuit breaker is open, and falls back to progressive behavior when the abandonment threshold is reached or the agent pool is too small.
3. **Per-call enforcement at the allocation boundary:** Immediately before each provider request, the allocator re-collects live metrics, asks the Safety Controller to approve one call, and atomically reserves one AVAILABLE agent. This second check handles agent drop-offs and concurrent workers after the original batch decision.
4. **No unsupported call-parking assumption:** This prototype does not claim that a virtual reservation or a compliant hold/whisper bridge guarantees an agent will become free. Every outbound call therefore has a real agent reserved before dialing. That intentionally gives up speculative over-dialing until a production-grade parking/bridge mechanism and its compliance behavior can be implemented and tested.

The result is a system where predictive logic can improve batching and pacing decisions, while the safety boundary—not the prediction—controls whether each call is allowed to start. The trade-off is explicit: lower potential utilization than aggressive predictive dialing, but no intentional dialing beyond currently reservable agent capacity.
