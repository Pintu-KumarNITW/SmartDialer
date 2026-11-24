from dataclasses import dataclass
from enum import Enum
from .pacing import PacingMetrics, PacingProposal


class SafetyDecisionType(str, Enum):
    APPROVE = "APPROVE"
    REDUCE = "REDUCE"
    REJECT = "REJECT"
    FALLBACK_PROGRESSIVE = "FALLBACK_PROGRESSIVE"


@dataclass
class SafetyDecision:
    decision: SafetyDecisionType
    approved_calls: int
    effective_mode: str
    reason: str


class SafetyController:
    """
    Independent hard safety boundary between the PacingEngine and CallAllocator.
    Cannot be disabled or bypassed by the predictive algorithm.

    Enforces:
    1. Provider circuit-breaker / outage protection
    2. Compliance abandonment rate ceiling -> forced fallback to PROGRESSIVE
    3. Hard in-flight concurrency ceiling relative to live AVAILABLE agents
       (protects immediately when agent availability suddenly drops)
    4. Strict 1:1 invariant when operating in PROGRESSIVE mode
    """

    def __init__(
        self,
        max_abandon_rate: float = 0.03,
        max_provider_error_rate: float = 0.35,
        max_inflight_to_available_ratio: float = 1.0,
        min_agents_for_predictive: int = 2,
    ):
        self.max_abandon_rate = max_abandon_rate
        self.max_provider_error_rate = max_provider_error_rate
        self.max_inflight_to_available_ratio = max_inflight_to_available_ratio
        self.min_agents_for_predictive = min_agents_for_predictive

    def evaluate(self, proposal: PacingProposal, m: PacingMetrics) -> SafetyDecision:
        if proposal.desired_calls <= 0:
            return SafetyDecision(
                decision=SafetyDecisionType.APPROVE,
                approved_calls=0,
                effective_mode=proposal.mode,
                reason="Zero calls requested",
            )

        # Rule 1: Provider Outage / High Error Rate Circuit Breaker
        if m.provider_error_rate >= self.max_provider_error_rate:
            return SafetyDecision(
                decision=SafetyDecisionType.REJECT,
                approved_calls=0,
                effective_mode="HALTED",
                reason=f"Circuit breaker open: provider error rate {m.provider_error_rate:.0%} >= {self.max_provider_error_rate:.0%}",
            )

        # Rule 2: Zero available agents -> immediate reject (handles sudden agent drops)
        if m.available_agents <= 0:
            return SafetyDecision(
                decision=SafetyDecisionType.REJECT,
                approved_calls=0,
                effective_mode=proposal.mode,
                reason="Zero AVAILABLE agents right now",
            )

        # Rule 3: Progressive mode strict invariant (1 available agent = max 1 unbound/new call)
        if proposal.mode == "PROGRESSIVE":
            allowed = min(proposal.desired_calls, m.available_agents, m.pending_borrowers)
            if allowed < proposal.desired_calls:
                return SafetyDecision(
                    decision=SafetyDecisionType.REDUCE,
                    approved_calls=allowed,
                    effective_mode="PROGRESSIVE",
                    reason=f"Clamped progressive calls to {allowed} available agents",
                )
            return SafetyDecision(
                decision=SafetyDecisionType.APPROVE,
                approved_calls=allowed,
                effective_mode="PROGRESSIVE",
                reason="Progressive invariant satisfied",
            )

        # Rule 4: Abandonment compliance breach or tiny agent pool -> Force fallback to PROGRESSIVE
        if (
            m.recent_abandon_rate >= self.max_abandon_rate
            or m.available_agents < self.min_agents_for_predictive
        ):
            safe_progressive = min(proposal.desired_calls, m.available_agents, m.pending_borrowers)
            return SafetyDecision(
                decision=SafetyDecisionType.FALLBACK_PROGRESSIVE,
                approved_calls=safe_progressive,
                effective_mode="PROGRESSIVE",
                reason=(
                    f"Fallback to PROGRESSIVE (abandon={m.recent_abandon_rate:.1%}, "
                    f"avail_agents={m.available_agents})"
                ),
            )

        # Rule 5: Hard cap on new calls. The allocator reserves a real agent
        # before dialing in BOTH modes, so only currently AVAILABLE agents are
        # eligible capacity. This deliberately prefers deterministic safety
        # over speculative over-dialing.
        in_flight = m.initiated_calls + m.ringing_calls
        max_total_inflight = int(m.available_agents * self.max_inflight_to_available_ratio)
        headroom = max(0, max_total_inflight)

        if headroom == 0:
            return SafetyDecision(
                decision=SafetyDecisionType.REJECT,
                approved_calls=0,
                effective_mode="PREDICTIVE",
                reason=f"No unreserved agent capacity (available={m.available_agents}, in_flight={in_flight})",
            )

        if proposal.desired_calls > headroom:
            return SafetyDecision(
                decision=SafetyDecisionType.REDUCE,
                approved_calls=headroom,
                effective_mode="PREDICTIVE",
                reason=f"Reduced {proposal.desired_calls} -> {headroom} to match unreserved agent capacity ({max_total_inflight})",
            )

        return SafetyDecision(
            decision=SafetyDecisionType.APPROVE,
            approved_calls=proposal.desired_calls,
            effective_mode="PREDICTIVE",
            reason="Within all safety & compliance thresholds",
        )
