from concurrent.futures import ThreadPoolExecutor
import time
import unittest

from src.dialer import SmartDialer
from src.models import Agent, AgentState, Borrower, Call, CallState, ProviderEvent
from src.pacing import PacingEngine
from src.providers import MockTelecomProvider
from src.safety import SafetyController, SafetyDecisionType
from src.store import StateStore


class TestSmartDialer(unittest.TestCase):
    def setUp(self):
        self.store = StateStore(":memory:")
        self.provider_a = MockTelecomProvider(
            name="ProviderA",
            latency_range_sec=(0.001, 0.002),
            failure_rate=0.0,
            auto_simulate_lifecycle=False,
        )
        self.provider_b = MockTelecomProvider(
            name="ProviderB",
            latency_range_sec=(0.002, 0.004),
            failure_rate=0.0,
            auto_simulate_lifecycle=False,
        )
        self.pacing = PacingEngine(mode="PROGRESSIVE")
        self.safety = SafetyController()
        self.dialer = SmartDialer(
            self.store, [self.provider_a, self.provider_b], self.pacing, self.safety
        )

    def test_1_concurrent_agent_reservation_race_condition(self):
        """
        10 concurrent workers race to reserve 1 AVAILABLE agent.
        Strictly 1 must succeed and 9 must fail.
        """
        self.store.add_agent(Agent(agent_id="ag-1", name="Alice"))

        def try_reserve(worker_idx: int):
            return self.store.atomic_reserve_agent(call_id=f"call-{worker_idx}")

        with ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(try_reserve, range(10)))

        winners = [r for r in results if r is not None]
        self.assertEqual(len(winners), 1)
        agent = self.store.get_agent("ag-1")
        self.assertEqual(agent.state, AgentState.RESERVED)

    def test_2_progressive_mode_never_exceeds_available_agents(self):
        """
        5 available agents and 20 borrowers -> strictly 5 calls placed.
        """
        for i in range(5):
            self.store.add_agent(Agent(agent_id=f"ag-{i}", name=f"Agent-{i}"))
        for i in range(20):
            self.store.add_borrower(
                Borrower(borrower_id=f"br-{i}", phone_number=f"+910000{i}", name=f"B-{i}")
            )

        decision = self.dialer.tick()
        self.assertEqual(decision.approved_calls, 5)
        self.assertEqual(len(self.store.list_calls()), 5)
        self.assertEqual(len(self.store.list_agents(AgentState.AVAILABLE)), 0)

        # Next tick with 0 available agents must place 0 calls
        decision2 = self.dialer.tick()
        self.assertEqual(decision2.approved_calls, 0)
        self.assertEqual(len(self.store.list_calls()), 5)

    def test_3_duplicate_and_out_of_order_events(self):
        """
        Provider sends:
        - Duplicate ANSWERED events
        - Out-of-order COMPLETED -> ANSWERED -> RINGING
        System must stay consistent without regressing state.
        """
        self.store.add_agent(Agent(agent_id="ag-1", name="Alice"))
        self.store.add_borrower(Borrower(borrower_id="br-1", phone_number="+91111", name="Bob"))

        call = self.dialer.allocate_and_dial(mode="PROGRESSIVE")
        self.assertIsNotNone(call)
        pid = call.provider_call_id

        # Duplicate ANSWERED with same event_id AND different event_id
        evt_ans_1 = ProviderEvent(event_id="e1", provider_call_id=pid, event_type="ANSWERED")
        self.dialer.handle_provider_event(evt_ans_1)
        self.dialer.handle_provider_event(evt_ans_1)  # exact duplicate
        self.dialer.handle_provider_event(
            ProviderEvent(event_id="e2", provider_call_id=pid, event_type="ANSWERED")
        )

        c = self.store.get_call(call.call_id)
        self.assertEqual(c.state, CallState.CONNECTED)
        self.assertEqual(self.store.get_agent("ag-1").state, AgentState.CONNECTED)

        # Out-of-order late RINGING arrives while CONNECTED
        self.dialer.handle_provider_event(
            ProviderEvent(event_id="e3", provider_call_id=pid, event_type="RINGING")
        )
        self.assertEqual(self.store.get_call(call.call_id).state, CallState.CONNECTED)

        # COMPLETED arrives, followed by stale ANSWERED and RINGING
        self.dialer.handle_provider_event(
            ProviderEvent(event_id="e4", provider_call_id=pid, event_type="COMPLETED")
        )
        self.dialer.handle_provider_event(
            ProviderEvent(event_id="e5", provider_call_id=pid, event_type="ANSWERED")
        )
        self.dialer.handle_provider_event(
            ProviderEvent(event_id="e6", provider_call_id=pid, event_type="RINGING")
        )

        final_call = self.store.get_call(call.call_id)
        self.assertEqual(final_call.state, CallState.COMPLETED)
        self.assertEqual(self.store.get_agent("ag-1").state, AgentState.AVAILABLE)
        self.assertEqual(self.store.get_borrower("br-1").status, "DONE")

    def test_4_worker_crash_recovery(self):
        """
        Agent reserved -> Borrower reserved -> Worker crashes before placing call.
        Sweeper recovers orphaned agent and requeues borrower.
        """
        self.store.add_agent(Agent(agent_id="ag-1", name="Alice"))
        self.store.add_borrower(Borrower(borrower_id="br-1", phone_number="+91111", name="Bob"))

        crashed_call = self.dialer.allocate_and_dial(
            mode="PROGRESSIVE", simulate_crash_before_dial=True
        )
        self.assertEqual(crashed_call.state, CallState.RESERVED)
        self.assertEqual(self.store.get_agent("ag-1").state, AgentState.DIALING)

        time.sleep(0.05)
        stats = self.dialer.recover_stale_resources(stale_timeout_sec=0.02)
        self.assertEqual(stats["recovered_calls"], 1)
        self.assertEqual(stats["recovered_agents"], 1)
        self.assertEqual(self.store.get_agent("ag-1").state, AgentState.AVAILABLE)
        self.assertEqual(self.store.get_borrower("br-1").status, "RETRY")

    def test_5_worker_crash_after_answered_then_completed_arrives(self):
        """
        Provider sends ANSWERED, worker crashes, then COMPLETED arrives on another worker.
        """
        self.store.add_agent(Agent(agent_id="ag-1", name="Alice"))
        self.store.add_borrower(Borrower(borrower_id="br-1", phone_number="+91111", name="Bob"))
        call = self.dialer.allocate_and_dial(mode="PROGRESSIVE")
        pid = call.provider_call_id

        self.dialer.handle_provider_event(
            ProviderEvent(event_id="e-ans", provider_call_id=pid, event_type="ANSWERED")
        )
        # Simulate worker 1 dying; worker 2 shares the same StateStore
        worker2 = SmartDialer(
            self.store, [self.provider_a, self.provider_b], self.pacing, self.safety, worker_id="worker-2"
        )
        worker2.handle_provider_event(
            ProviderEvent(event_id="e-comp", provider_call_id=pid, event_type="COMPLETED")
        )

        self.assertEqual(self.store.get_call(call.call_id).state, CallState.COMPLETED)
        self.assertEqual(self.store.get_agent("ag-1").state, AgentState.AVAILABLE)
        self.assertEqual(self.store.get_borrower("br-1").status, "DONE")

    def test_6_provider_outage_failover_and_circuit_breaker(self):
        """
        When Provider A goes down, dialer fails over to Provider B.
        When ALL providers fail, SafetyController opens circuit breaker.
        """
        for i in range(4):
            self.store.add_agent(Agent(agent_id=f"ag-{i}", name=f"Agent-{i}"))
        for i in range(10):
            self.store.add_borrower(
                Borrower(borrower_id=f"br-{i}", phone_number=f"+910000{i}", name=f"B-{i}")
            )

        self.provider_a.set_outage(True)
        # First call fails on A, records error, next calls route to healthy Provider B
        self.dialer.allocate_and_dial(mode="PROGRESSIVE")
        c2 = self.dialer.allocate_and_dial(mode="PROGRESSIVE")
        self.assertIsNotNone(c2)
        self.assertEqual(c2.provider_name, "ProviderB")

        # Now take down Provider B as well -> Circuit Breaker must REJECT new calls
        self.provider_b.set_outage(True)
        for _ in range(5):
            self.dialer.allocate_and_dial(mode="PROGRESSIVE")

        decision = self.dialer.tick()
        self.assertEqual(decision.decision, SafetyDecisionType.REJECT)
        self.assertEqual(decision.approved_calls, 0)

    def test_7_safety_controller_cannot_be_bypassed_and_handles_agent_drop(self):
        """
        Predictive engine requests 20 calls, SafetyController clamps/rejects when agents drop.
        """
        self.dialer.pacing_engine = PacingEngine(mode="PREDICTIVE")
        for i in range(10):
            self.store.add_agent(Agent(agent_id=f"ag-{i}", name=f"Agent-{i}"))
        for i in range(50):
            self.store.add_borrower(
                Borrower(borrower_id=f"br-{i}", phone_number=f"+910000{i}", name=f"B-{i}")
            )

        # Simulate low answer rate so predictive wants aggressive dialing
        for _ in range(20):
            self.dialer._recent_outcomes.append(False)

        decision = self.dialer.tick()
        self.assertLessEqual(decision.approved_calls, 10)  # one reserved agent per approved call

        # Suddenly drop remaining available agents to OFFLINE
        for ag in self.store.list_agents(AgentState.AVAILABLE):
            self.store.transition_agent(ag.agent_id, AgentState.OFFLINE)

        # Tick when 0 agents are available: both pacing & safety approve 0 calls
        decision_after_drop = self.dialer.tick()
        self.assertEqual(decision_after_drop.approved_calls, 0)

        # Even if a rogue/buggy pacing proposal demands 10 calls with 0 agents, SafetyController REJECTS it
        from src.pacing import PacingProposal
        forced = self.safety.evaluate(
            PacingProposal(mode="PREDICTIVE", desired_calls=10, reason="Aggressive burst"),
            self.dialer.collect_metrics(),
        )
        self.assertEqual(forced.decision, SafetyDecisionType.REJECT)
        self.assertEqual(forced.approved_calls, 0)


    def test_8_predictive_allocator_reserves_agent_before_dialing(self):
        """Predictive calls must also own a real agent before reaching the provider."""
        self.store.add_agent(Agent(agent_id="ag-p", name="Predictive Agent"))
        self.store.add_borrower(Borrower(borrower_id="br-p", phone_number="+91123", name="Predictive Borrower"))
        call = self.dialer.allocate_and_dial(mode="PREDICTIVE")
        self.assertIsNotNone(call)
        self.assertEqual(call.agent_id, "ag-p")
        self.assertEqual(self.store.get_agent("ag-p").state, AgentState.DIALING)

    def test_9_direct_allocator_cannot_bypass_zero_agent_safety(self):
        """A direct allocator invocation cannot place a call without agent capacity."""
        self.store.add_borrower(Borrower(borrower_id="br-no-agent", phone_number="+91456", name="No Agent"))
        call = self.dialer.allocate_and_dial(mode="PREDICTIVE")
        self.assertIsNone(call)
        self.assertEqual(len(self.store.list_calls()), 0)

    def test_10_progressive_agent_disappears_or_provider_fails_during_setup(self):
        """
        In Progressive mode, if the telecom provider fails during place_call,
        the reserved agent must be released back to AVAILABLE and the borrower marked RETRY.
        """
        self.store.add_agent(Agent(agent_id="ag-1", name="Alice"))
        self.store.add_borrower(
            Borrower(borrower_id="br-1", phone_number="+9100001", name="B-1")
        )

        # Force both providers to fail
        self.provider_a.set_outage(True)
        self.provider_b.set_outage(True)

        call = self.dialer.allocate_and_dial(mode="PROGRESSIVE")
        self.assertIsNone(call)

        # Agent must be cleanly returned to AVAILABLE (not stuck in RESERVED/DIALING)
        agent = self.store.get_agent("ag-1")
        self.assertEqual(agent.state, AgentState.AVAILABLE)
        self.assertIsNone(agent.current_call_id)

        # Borrower must be marked RETRY
        borrower = self.store.get_borrower("br-1")
        self.assertEqual(borrower.status, "RETRY")


if __name__ == "__main__":
    unittest.main()
