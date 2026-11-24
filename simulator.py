import time
from typing import Dict, List
from .dialer import SmartDialer
from .models import Agent, AgentState, Borrower, CallState
from .pacing import PacingEngine
from .providers import MockTelecomProvider
from .safety import SafetyController
from .store import StateStore


def run_scenario(
    name: str,
    mode: str = "PREDICTIVE",
    num_agents: int = 20,
    num_borrowers: int = 120,
    answer_rate: float = 0.50,
    talk_time_sec: float = 0.12,
    dynamic_shift: bool = False,
    inject_outage: bool = False,
    drop_agents_midway: bool = False,
    ticks: int = 25,
) -> Dict:
    store = StateStore(":memory:")
    for i in range(num_agents):
        store.add_agent(Agent(agent_id=f"ag-{i:03d}", name=f"Agent-{i}"))
    for i in range(num_borrowers):
        store.add_borrower(
            Borrower(borrower_id=f"br-{i:04d}", phone_number=f"+9190000{i:05d}", name=f"Borrower-{i}")
        )

    provider_a = MockTelecomProvider(
        name="ProviderA",
        latency_range_sec=(0.001, 0.003),
        failure_rate=0.01,
        answer_rate=answer_rate,
        ring_duration_sec=0.02,
        talk_duration_sec=talk_time_sec,
    )
    provider_b = MockTelecomProvider(
        name="ProviderB",
        latency_range_sec=(0.005, 0.012),
        failure_rate=0.05,
        timeout_rate=0.05,
        duplicate_event_prob=0.25,
        out_of_order_prob=0.20,
        answer_rate=answer_rate,
        ring_duration_sec=0.03,
        talk_duration_sec=talk_time_sec,
    )

    pacing = PacingEngine(mode=mode)
    safety = SafetyController()
    dialer = SmartDialer(store, [provider_a, provider_b], pacing, safety)

    busy_samples = []
    total_agent_samples = []

    for t in range(ticks):
        # Scenario D: Dynamic shift in answer rate & talk duration midway
        if dynamic_shift and t == ticks // 2:
            provider_a.answer_rate = 0.15
            provider_b.answer_rate = 0.15
            provider_a.talk_duration_sec = 0.22
            provider_b.talk_duration_sec = 0.22

        # Failure simulation: Provider A outage midway -> automatic failover to Provider B
        if inject_outage:
            if t == 8:
                provider_a.set_outage(True)
            elif t == 14:
                provider_a.set_outage(False)

        # Failure simulation: Sudden 40% agent drop
        if drop_agents_midway and t == 10:
            avail = store.list_agents(AgentState.AVAILABLE)
            for ag in avail[: max(1, int(len(avail) * 0.4))]:
                store.transition_agent(ag.agent_id, AgentState.OFFLINE, expected_states=[AgentState.AVAILABLE])

        dialer.tick()
        time.sleep(0.025)

        counts = store.count_agents_by_state()
        online = num_agents - counts[AgentState.OFFLINE]
        busy = counts[AgentState.CONNECTED] + counts[AgentState.DIALING] + counts[AgentState.RESERVED]
        if online > 0:
            busy_samples.append(busy)
            total_agent_samples.append(online)

    # Allow in-flight calls to settle
    time.sleep(talk_time_sec + 0.08)

    all_calls = store.list_calls()
    connected = sum(1 for c in all_calls if c.connected_at is not None)
    abandoned = sum(1 for c in all_calls if c.abandoned)
    failed = sum(1 for c in all_calls if c.state == CallState.FAILED)
    utilization = (sum(busy_samples) / sum(total_agent_samples) * 100) if total_agent_samples else 0.0

    decisions_summary: Dict[str, int] = {}
    for d in dialer.decision_history:
        decisions_summary[d["decision"]] = decisions_summary.get(d["decision"], 0) + 1

    return {
        "scenario": name,
        "mode": mode,
        "calls_initiated": len(all_calls),
        "calls_connected": connected,
        "calls_abandoned": abandoned,
        "calls_failed": failed,
        "abandon_rate_pct": round((abandoned / connected * 100) if connected else 0.0, 2),
        "agent_utilization_pct": round(utilization, 1),
        "safety_decisions": decisions_summary,
    }


def main():
    scenarios = [
        run_scenario("Baseline Progressive (50% ans, 120ms talk)", mode="PROGRESSIVE", answer_rate=0.50, talk_time_sec=0.12),
        run_scenario("Scenario A: Low Answer Rate (20% ans, 120ms talk)", mode="PREDICTIVE", answer_rate=0.20, talk_time_sec=0.12),
        run_scenario("Scenario B: Medium Answer Rate (50% ans, 90ms talk)", mode="PREDICTIVE", answer_rate=0.50, talk_time_sec=0.09),
        run_scenario("Scenario C: High Answer Rate (70% ans, 180ms talk)", mode="PREDICTIVE", answer_rate=0.70, talk_time_sec=0.18),
        run_scenario("Scenario D: Dynamic Shift + Latency/Failures", mode="PREDICTIVE", answer_rate=0.65, talk_time_sec=0.09, dynamic_shift=True, inject_outage=True),
        run_scenario("Scenario E: Sudden 40% Agent Drop", mode="PREDICTIVE", answer_rate=0.50, talk_time_sec=0.12, drop_agents_midway=True),
    ]

    print("=" * 108)
    print(f"{'Scenario':<48} | {'Init':>5} | {'Conn':>5} | {'Abnd':>4} | {'Util%':>6} | {'Safety Decisions'}")
    print("-" * 108)
    for r in scenarios:
        print(
            f"{r['scenario']:<48} | {r['calls_initiated']:>5} | {r['calls_connected']:>5} | "
            f"{r['calls_abandoned']:>4} | {r['agent_utilization_pct']:>5.1f}% | {r['safety_decisions']}"
        )
    print("=" * 108)


if __name__ == "__main__":
    main()
