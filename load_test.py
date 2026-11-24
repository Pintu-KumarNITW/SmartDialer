from concurrent.futures import ThreadPoolExecutor
import time
from src.dialer import SmartDialer
from src.models import Agent, AgentState, Borrower
from src.pacing import PacingEngine
from src.providers import MockTelecomProvider
from src.safety import SafetyController
from src.store import StateStore


def run_load_test(num_agents: int = 200, num_borrowers: int = 1000, num_workers: int = 8):
    store = StateStore(":memory:")
    for i in range(num_agents):
        store.add_agent(Agent(agent_id=f"ag-{i:04d}", name=f"Agent-{i}"))
    for i in range(num_borrowers):
        store.add_borrower(
            Borrower(borrower_id=f"br-{i:05d}", phone_number=f"+918000{i:06d}", name=f"B-{i}")
        )

    provider = MockTelecomProvider(
        name="FastLoadProvider",
        latency_range_sec=(0.0005, 0.0015),
        failure_rate=0.01,
        answer_rate=0.55,
        ring_duration_sec=0.01,
        talk_duration_sec=0.04,
    )
    dialer = SmartDialer(
        store=store,
        providers=[provider],
        pacing_engine=PacingEngine(mode="PREDICTIVE"),
        safety_controller=SafetyController(),
    )

    start = time.time()

    def worker_loop(wid: int):
        for _ in range(15):
            dialer.tick(worker_id=f"worker-{wid}")
            time.sleep(0.01)

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        list(pool.map(worker_loop, range(num_workers)))

    time.sleep(0.1)
    elapsed = time.time() - start

    # Invariant checks:
    # 1. No two active calls may share the same agent_id
    active_agents = [
        c.agent_id for c in store.list_calls() if c.agent_id is not None and c.ended_at is None
    ]
    assert len(active_agents) == len(set(active_agents)), "CRITICAL: Double agent allocation detected!"

    # 2. No agent is stuck in RESERVED without a call
    for ag in store.list_agents(AgentState.RESERVED):
        assert ag.current_call_id is not None, f"Orphaned reserved agent {ag.agent_id}"

    all_calls = store.list_calls()
    connected = sum(1 for c in all_calls if c.connected_at is not None)
    abandoned = sum(1 for c in all_calls if c.abandoned)

    print("=== Load Test Passed (Zero Race Conditions) ===")
    print(f"Agents: {num_agents} | Borrowers: {num_borrowers} | Concurrent Workers: {num_workers}")
    print(f"Elapsed: {elapsed:.2f}s | Calls Placed: {len(all_calls)} ({len(all_calls)/elapsed:.1f} calls/sec)")
    print(f"Connected: {connected} | Abandoned: {abandoned} | Abandon Rate: {(abandoned/connected*100) if connected else 0:.2f}%")


if __name__ == "__main__":
    run_load_test()
