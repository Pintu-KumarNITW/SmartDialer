from collections import deque
import threading
import time
from typing import Deque, Dict, List, Optional
import uuid

from .models import (
    AgentState,
    Call,
    CallState,
    ProviderEvent,
    TERMINAL_CALL_STATES,
)
from .pacing import PacingEngine, PacingMetrics
from .providers import ProviderError, TelecomProvider
from .safety import SafetyController, SafetyDecision
from .store import StateStore


class SmartDialer:
    """
    Coordinates Campaign -> PacingEngine -> SafetyController -> CallAllocator -> TelecomProvider,
    and processes asynchronous webhook events + crash/stale recovery.
    """

    def __init__(
        self,
        store: StateStore,
        providers: List[TelecomProvider],
        pacing_engine: PacingEngine,
        safety_controller: SafetyController,
        worker_id: str = "worker-1",
    ):
        self.store = store
        self.providers = providers
        self.pacing_engine = pacing_engine
        self.safety_controller = safety_controller
        self.worker_id = worker_id

        for p in self.providers:
            p.register_webhook(self.handle_provider_event)

        # Rolling telemetry windows for predictive estimation
        self._recent_outcomes: Deque[bool] = deque(maxlen=50)  # True = answered
        self._recent_abandons: Deque[bool] = deque(maxlen=50)  # True = answered but no agent
        self._recent_talk_times: Deque[float] = deque(maxlen=50)
        self._recent_setup_times: Deque[float] = deque(maxlen=50)
        self._lock = threading.Lock()

        # Audit log of pacing & safety decisions for observability
        self.decision_history: List[dict] = []

    def select_healthy_provider(self) -> TelecomProvider:
        """Routes to lowest error-rate / fastest provider."""
        return min(self.providers, key=lambda p: (p.error_rate, p.avg_latency_ms))

    def collect_metrics(self) -> PacingMetrics:
        agent_counts = self.store.count_agents_by_state()
        active_calls = self.store.list_calls(
            [CallState.RESERVED, CallState.INITIATED, CallState.RINGING, CallState.ANSWERED, CallState.CONNECTED]
        )

        initiated = sum(1 for c in active_calls if c.state in (CallState.RESERVED, CallState.INITIATED))
        ringing = sum(1 for c in active_calls if c.state == CallState.RINGING)
        connected = sum(1 for c in active_calls if c.state in (CallState.ANSWERED, CallState.CONNECTED))

        with self._lock:
            ans_rate = (
                sum(1 for x in self._recent_outcomes if x) / len(self._recent_outcomes)
                if self._recent_outcomes
                else 0.50
            )
            abandon_rate = (
                sum(1 for x in self._recent_abandons if x) / len(self._recent_abandons)
                if self._recent_abandons
                else 0.0
            )
            avg_talk = (
                sum(self._recent_talk_times) / len(self._recent_talk_times)
                if self._recent_talk_times
                else 0.12
            )
            avg_setup = (
                sum(self._recent_setup_times) / len(self._recent_setup_times)
                if self._recent_setup_times
                else 0.04
            )

        best_provider = self.select_healthy_provider()

        return PacingMetrics(
            available_agents=agent_counts[AgentState.AVAILABLE],
            reserved_agents=agent_counts[AgentState.RESERVED],
            connected_calls=connected,
            ringing_calls=ringing,
            initiated_calls=initiated,
            pending_borrowers=self.store.count_pending_borrowers(),
            estimated_answer_rate=ans_rate,
            avg_talk_time_sec=avg_talk,
            avg_setup_time_sec=avg_setup,
            provider_error_rate=best_provider.error_rate,
            recent_abandon_rate=abandon_rate,
        )

    def tick(self, worker_id: Optional[str] = None) -> SafetyDecision:
        """
        Single control-loop step:
        1. Collect live system state
        2. Ask PacingEngine for desired call count
        3. Pass proposal through SafetyController
        4. Execute approved allocations via CallAllocator
        """
        wid = worker_id or self.worker_id
        metrics = self.collect_metrics()
        proposal = self.pacing_engine.compute_desired_calls(metrics)
        decision = self.safety_controller.evaluate(proposal, metrics)

        placed = 0
        for _ in range(decision.approved_calls):
            call = self.allocate_and_dial(mode=decision.effective_mode, worker_id=wid)
            if call:
                placed += 1
            else:
                break

        self.decision_history.append(
            {
                "timestamp": time.time(),
                "worker_id": wid,
                "available_agents": metrics.available_agents,
                "connected_calls": metrics.connected_calls,
                "ringing_calls": metrics.ringing_calls,
                "est_answer_rate": round(metrics.estimated_answer_rate, 3),
                "abandon_rate": round(metrics.recent_abandon_rate, 3),
                "proposed": proposal.desired_calls,
                "decision": decision.decision.value,
                "approved": decision.approved_calls,
                "placed": placed,
                "reason": decision.reason,
            }
        )
        return decision

    def allocate_and_dial(
        self,
        mode: str,
        worker_id: Optional[str] = None,
        simulate_crash_before_dial: bool = False,
    ) -> Optional[Call]:
        """
        Call Allocator:
        - In BOTH modes: re-checks SafetyController, atomically reserves an
          Agent FIRST, then claims a Borrower. If borrower missing or agent
          drops out, rolls back cleanly.
        - Predictive pacing may request a larger batch, but cannot start a
          call without a concrete reserved agent.
        """
        wid = worker_id or self.worker_id
        # Enforce the safety boundary again at the allocation edge. This closes
        # the gap between a batch approval and a concurrent agent-state change,
        # and prevents callers from invoking the allocator to bypass the gate.
        live_metrics = self.collect_metrics()
        from .pacing import PacingProposal
        allocation_gate = self.safety_controller.evaluate(
            PacingProposal(mode=mode, desired_calls=1, reason="Single-call allocation gate"),
            live_metrics,
        )
        if allocation_gate.approved_calls < 1:
            return None

        call_id = f"call-{uuid.uuid4().hex[:10]}"
        reserved_agent_id: Optional[str] = None

        # Both modes reserve a concrete agent before the provider is called.
        # Predictive pacing may choose a larger batch, but it cannot speculate
        # beyond real agent capacity. This deterministic reservation prevents
        # two simultaneous ANSWERED events from competing for one free agent.
        agent = self.store.atomic_reserve_agent(call_id=call_id)
        if not agent:
            return None
        reserved_agent_id = agent.agent_id

        borrower = self.store.atomic_claim_borrower()
        if not borrower:
            if reserved_agent_id:
                self.store.transition_agent(
                    reserved_agent_id,
                    AgentState.AVAILABLE,
                    expected_states=[AgentState.RESERVED],
                    clear_call=True,
                )
            return None

        call = Call(
            call_id=call_id,
            borrower_id=borrower.borrower_id,
            mode=mode,
            agent_id=reserved_agent_id,
            state=CallState.RESERVED,
            worker_id=wid,
        )
        self.store.create_call(call)

        if reserved_agent_id:
            # Verify agent hasn't gone OFFLINE/PAUSED right before dialing
            ok = self.store.transition_agent(
                reserved_agent_id,
                AgentState.DIALING,
                expected_states=[AgentState.RESERVED],
                call_id=call_id,
            )
            if not ok:
                # Agent disappeared during call setup -> cancel call and requeue borrower
                self.store.advance_call_state(
                    call_id, CallState.CANCELLED, failure_reason="Agent unavailable during setup"
                )
                self.store.update_borrower_status(borrower.borrower_id, "RETRY")
                return None

        if simulate_crash_before_dial:
            # Simulates worker dying right after reserving resources and before/during initiation
            return call

        provider = self.select_healthy_provider()
        try:
            provider_call_id = provider.place_call(call_id, borrower.phone_number)
            self.store.advance_call_state(
                call_id,
                CallState.INITIATED,
                provider_call_id=provider_call_id,
                provider_name=provider.name,
            )
            self.store.update_borrower_status(borrower.borrower_id, "IN_PROGRESS")
            return self.store.get_call(call_id)
        except ProviderError as exc:
            self.store.advance_call_state(
                call_id, CallState.FAILED, failure_reason=str(exc)
            )
            self.store.update_borrower_status(borrower.borrower_id, "RETRY")
            if reserved_agent_id:
                self.store.transition_agent(
                    reserved_agent_id,
                    AgentState.AVAILABLE,
                    expected_states=[AgentState.RESERVED, AgentState.DIALING],
                    clear_call=True,
                )
            return None

    def handle_provider_event(self, event: ProviderEvent) -> Optional[Call]:
        """
        Idempotent, out-of-order-safe webhook handler.
        """
        # 1. Deduplicate identical webhook retries
        if not self.store.record_event(event.event_id, event.provider_call_id, event.event_type):
            return self.store.get_call_by_provider_id(event.provider_call_id)

        call = self.store.get_call_by_provider_id(event.provider_call_id)
        if not call:
            return None

        etype = event.event_type.upper()

        if etype == "RINGING":
            # Monotonic advance_call_state automatically ignores RINGING if already ANSWERED/COMPLETED
            return self.store.advance_call_state(call.call_id, CallState.RINGING)

        if etype == "ANSWERED":
            # Guard against duplicate ANSWERED events with different event_ids
            if call.state in (CallState.ANSWERED, CallState.CONNECTED) or call.state in TERMINAL_CALL_STATES:
                return call

            updated = self.store.advance_call_state(call.call_id, CallState.ANSWERED)
            if not updated:
                return None

            with self._lock:
                self._recent_outcomes.append(True)
                if updated.answered_at:
                    self._recent_setup_times.append(max(0.005, updated.answered_at - updated.created_at))

            # Bind an agent (either pre-reserved in PROGRESSIVE or dynamically in PREDICTIVE)
            agent_id = updated.agent_id
            if not agent_id:
                agent = self.store.atomic_reserve_agent(call_id=updated.call_id)
                if agent:
                    agent_id = agent.agent_id

            if agent_id:
                connected = self.store.transition_agent(
                    agent_id,
                    AgentState.CONNECTED,
                    expected_states=[AgentState.RESERVED, AgentState.DIALING],
                    call_id=updated.call_id,
                )
                if connected:
                    with self._lock:
                        self._recent_abandons.append(False)
                    return self.store.advance_call_state(
                        updated.call_id, CallState.CONNECTED, agent_id=agent_id
                    )

            # No available agent when borrower answered -> ABANDONED CALL (compliance event)
            with self._lock:
                self._recent_abandons.append(True)
            abandoned_call = self.store.advance_call_state(
                updated.call_id,
                CallState.CANCELLED,
                abandoned=True,
                failure_reason="ABANDONED_NO_AGENT_AVAILABLE",
            )
            self.store.update_borrower_status(updated.borrower_id, "RETRY")
            for p in self.providers:
                if p.name == updated.provider_name and updated.provider_call_id:
                    p.hangup_call(updated.provider_call_id)
            return abandoned_call

        if etype == "COMPLETED":
            # Handles cases where COMPLETED arrives after a worker crash or out of order
            was_answered = call.answered_at is not None or call.state in (
                CallState.ANSWERED,
                CallState.CONNECTED,
            )
            completed = self.store.advance_call_state(call.call_id, CallState.COMPLETED)
            if completed and completed.agent_id:
                self.store.transition_agent(
                    completed.agent_id,
                    AgentState.AVAILABLE,  # Simplified wrap-up -> available for continuous simulation
                    expected_states=[AgentState.CONNECTED, AgentState.RESERVED, AgentState.DIALING, AgentState.WRAP_UP],
                    clear_call=True,
                )
            self.store.update_borrower_status(
                call.borrower_id, "DONE" if was_answered else "RETRY"
            )
            if completed and completed.connected_at and completed.ended_at:
                with self._lock:
                    self._recent_talk_times.append(max(0.01, completed.ended_at - completed.connected_at))
            return completed

        if etype in ("FAILED", "CANCELLED"):
            target = CallState.FAILED if etype == "FAILED" else CallState.CANCELLED
            terminal = self.store.advance_call_state(
                call.call_id, target, failure_reason=event.reason or etype
            )
            with self._lock:
                self._recent_outcomes.append(False)
            if terminal and terminal.agent_id:
                self.store.transition_agent(
                    terminal.agent_id,
                    AgentState.AVAILABLE,
                    expected_states=[AgentState.RESERVED, AgentState.DIALING, AgentState.CONNECTED],
                    clear_call=True,
                )
            self.store.update_borrower_status(call.borrower_id, "RETRY")
            return terminal

        return call

    def recover_stale_resources(self, stale_timeout_sec: float = 5.0) -> Dict[str, int]:
        """
        Reconciler / Sweeper process for worker crashes:
        - Finds calls stuck in RESERVED/INITIATED/RINGING/ANSWERED beyond timeout
        - Releases orphaned RESERVED/DIALING agents back to AVAILABLE
        - Requeues affected borrowers for retry
        """
        now = time.time()
        recovered_calls = 0
        recovered_agents = 0

        active_calls = self.store.list_calls(
            [CallState.RESERVED, CallState.INITIATED, CallState.RINGING, CallState.ANSWERED]
        )
        for c in active_calls:
            if (now - c.updated_at) >= stale_timeout_sec:
                self.store.advance_call_state(
                    c.call_id, CallState.FAILED, failure_reason="WORKER_OR_PROVIDER_TIMEOUT"
                )
                self.store.update_borrower_status(c.borrower_id, "RETRY")
                if c.agent_id:
                    if self.store.transition_agent(
                        c.agent_id,
                        AgentState.AVAILABLE,
                        expected_states=[AgentState.RESERVED, AgentState.DIALING, AgentState.CONNECTED],
                        clear_call=True,
                    ):
                        recovered_agents += 1
                recovered_calls += 1

        # Also sweep any agent stuck in RESERVED/DIALING whose call is missing or terminal
        for state in (AgentState.RESERVED, AgentState.DIALING):
            for agent in self.store.list_agents(state):
                if (now - agent.updated_at) >= stale_timeout_sec:
                    call = self.store.get_call(agent.current_call_id) if agent.current_call_id else None
                    if call is None or call.state in TERMINAL_CALL_STATES:
                        if self.store.transition_agent(
                            agent.agent_id,
                            AgentState.AVAILABLE,
                            expected_states=[AgentState.RESERVED, AgentState.DIALING],
                            clear_call=True,
                        ):
                            recovered_agents += 1

        return {"recovered_calls": recovered_calls, "recovered_agents": recovered_agents}
