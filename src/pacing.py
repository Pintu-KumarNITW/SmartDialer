from dataclasses import dataclass
import math
from typing import Dict


@dataclass
class PacingMetrics:
    available_agents: int
    reserved_agents: int
    connected_calls: int
    ringing_calls: int
    initiated_calls: int
    pending_borrowers: int
    estimated_answer_rate: float
    avg_talk_time_sec: float
    avg_setup_time_sec: float
    provider_error_rate: float
    recent_abandon_rate: float


@dataclass
class PacingProposal:
    mode: str
    desired_calls: int
    reason: str


class PacingEngine:
    """
    Computes how many new outbound calls the campaign wants to start right now.
    NEVER places calls directly; its output must pass through the SafetyController.
    """

    def __init__(
        self,
        mode: str = "PREDICTIVE",
        target_abandon_cap: float = 0.03,
        max_overdial_ratio: float = 2.5,
    ):
        self.mode = mode.upper()
        self.target_abandon_cap = target_abandon_cap
        self.max_overdial_ratio = max_overdial_ratio

    def compute_desired_calls(self, m: PacingMetrics) -> PacingProposal:
        if m.pending_borrowers <= 0:
            return PacingProposal(self.mode, 0, "No pending borrowers in queue")

        if self.mode == "PROGRESSIVE":
            # Strictly 1:1 with currently available agents
            desired = min(m.available_agents, m.pending_borrowers)
            return PacingProposal(
                mode="PROGRESSIVE",
                desired_calls=max(0, desired),
                reason=f"Progressive 1:1 match for {m.available_agents} available agents",
            )

        # --- PREDICTIVE MODE ---
        # 1. Estimate effective capacity:
        #    Currently available agents + agents expected to finish active calls during call setup window
        if m.avg_talk_time_sec > 0 and m.connected_calls > 0:
            completion_prob = min(0.5, m.avg_setup_time_sec / m.avg_talk_time_sec)
            expected_freeing_soon = m.connected_calls * completion_prob
        else:
            expected_freeing_soon = 0.0

        effective_agent_supply = m.available_agents + expected_freeing_soon

        if effective_agent_supply <= 0.2:
            return PacingProposal(
                mode="PREDICTIVE",
                desired_calls=0,
                reason="Insufficient effective agent supply (available + soon-to-free)",
            )

        # 2. Account for calls already in flight (INITIATED + RINGING)
        in_flight = m.initiated_calls + m.ringing_calls
        ans_rate = max(0.10, min(0.95, m.estimated_answer_rate))
        expected_answers_in_flight = in_flight * ans_rate

        remaining_capacity = effective_agent_supply - expected_answers_in_flight
        if remaining_capacity <= 0:
            return PacingProposal(
                mode="PREDICTIVE",
                desired_calls=0,
                reason=(
                    f"In-flight expected answers ({expected_answers_in_flight:.2f}) "
                    f"already fill supply ({effective_agent_supply:.2f})"
                ),
            )

        # 3. Scale desired calls inversely with answer rate, bounded by overdial ratio
        raw_multiplier = min(self.max_overdial_ratio, 1.0 / ans_rate)

        # Dampen multiplier if recent abandon rate approaches compliance cap
        if m.recent_abandon_rate > self.target_abandon_cap * 0.5:
            dampen = max(0.4, 1.0 - (m.recent_abandon_rate / self.target_abandon_cap))
            raw_multiplier = 1.0 + (raw_multiplier - 1.0) * dampen

        # Dampen if provider health degrades
        if m.provider_error_rate > 0.10:
            raw_multiplier = min(raw_multiplier, 1.0)

        desired = math.floor(remaining_capacity * raw_multiplier)
        desired = min(desired, m.pending_borrowers)

        reason = (
            f"Supply={effective_agent_supply:.1f} (avail={m.available_agents}, "
            f"freeing={expected_freeing_soon:.1f}), in_flight={in_flight} "
            f"(exp_ans={expected_answers_in_flight:.1f}), ans_rate={ans_rate:.0%}, "
            f"mult={raw_multiplier:.2f}x -> desire {desired}"
        )
        return PacingProposal(mode="PREDICTIVE", desired_calls=max(0, desired), reason=reason)
