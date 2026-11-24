from abc import ABC, abstractmethod
from collections import deque
import random
import threading
import time
from typing import Callable, Deque, Dict, Optional
import uuid
from .models import ProviderEvent


class ProviderError(Exception):
    pass


class ProviderTimeoutError(ProviderError):
    pass


class TelecomProvider(ABC):
    """Abstract Telecom Provider Interface hiding vendor-specific quirks."""

    def __init__(self, name: str):
        self.name = name
        self._callback: Optional[Callable[[ProviderEvent], None]] = None
        self._recent_outcomes: Deque[bool] = deque(maxlen=20)
        self._latencies_ms: Deque[float] = deque(maxlen=20)
        self._lock = threading.Lock()

    def register_webhook(self, callback: Callable[[ProviderEvent], None]):
        self._callback = callback

    def record_outcome(self, success: bool, latency_ms: float):
        with self._lock:
            self._recent_outcomes.append(success)
            self._latencies_ms.append(latency_ms)

    @property
    def error_rate(self) -> float:
        with self._lock:
            if not self._recent_outcomes:
                return 0.0
            failures = sum(1 for ok in self._recent_outcomes if not ok)
            return failures / len(self._recent_outcomes)

    @property
    def avg_latency_ms(self) -> float:
        with self._lock:
            if not self._latencies_ms:
                return 10.0
            return sum(self._latencies_ms) / len(self._latencies_ms)

    @abstractmethod
    def place_call(self, call_id: str, phone_number: str) -> str:
        """Initiates outbound call and returns provider_call_id."""
        pass

    @abstractmethod
    def hangup_call(self, provider_call_id: str) -> bool:
        """Terminates an active/ringing call."""
        pass


class MockTelecomProvider(TelecomProvider):
    """
    Configurable Mock Telecom Provider supporting:
    - Provider A profile: fast, reliable, low failure rate
    - Provider B profile: slower, timeouts, duplicate webhooks, out-of-order events
    - Dynamic runtime overrides (answer rate, talk time, outage injection)
    """

    def __init__(
        self,
        name: str,
        latency_range_sec: tuple = (0.005, 0.015),
        failure_rate: float = 0.02,
        timeout_rate: float = 0.0,
        duplicate_event_prob: float = 0.0,
        out_of_order_prob: float = 0.0,
        answer_rate: float = 0.50,
        ring_duration_sec: float = 0.04,
        talk_duration_sec: float = 0.12,
        auto_simulate_lifecycle: bool = True,
    ):
        super().__init__(name)
        self.latency_range_sec = latency_range_sec
        self.failure_rate = failure_rate
        self.timeout_rate = timeout_rate
        self.duplicate_event_prob = duplicate_event_prob
        self.out_of_order_prob = out_of_order_prob
        self.answer_rate = answer_rate
        self.ring_duration_sec = ring_duration_sec
        self.talk_duration_sec = talk_duration_sec
        self.auto_simulate_lifecycle = auto_simulate_lifecycle
        self.is_down = False
        self._active_timers: Dict[str, threading.Event] = {}

    def set_outage(self, down: bool):
        self.is_down = down

    def place_call(self, call_id: str, phone_number: str) -> str:
        start = time.time()
        delay = random.uniform(*self.latency_range_sec)
        time.sleep(delay)

        if self.is_down or random.random() < self.timeout_rate:
            self.record_outcome(False, (time.time() - start) * 1000)
            raise ProviderTimeoutError(f"[{self.name}] Request timed out for call {call_id}")

        if random.random() < self.failure_rate:
            self.record_outcome(False, (time.time() - start) * 1000)
            raise ProviderError(f"[{self.name}] Trunk failure for call {call_id}")

        self.record_outcome(True, (time.time() - start) * 1000)
        provider_call_id = f"{self.name}-{uuid.uuid4().hex[:10]}"

        if self.auto_simulate_lifecycle and self._callback:
            cancel_flag = threading.Event()
            self._active_timers[provider_call_id] = cancel_flag
            t = threading.Thread(
                target=self._run_call_lifecycle,
                args=(provider_call_id, cancel_flag),
                daemon=True,
            )
            t.start()

        return provider_call_id

    def hangup_call(self, provider_call_id: str) -> bool:
        flag = self._active_timers.get(provider_call_id)
        if flag:
            flag.set()
        return True

    def _emit(self, provider_call_id: str, event_type: str, reason: Optional[str] = None):
        if not self._callback:
            return
        evt = ProviderEvent(
            event_id=f"evt-{uuid.uuid4().hex[:12]}",
            provider_call_id=provider_call_id,
            event_type=event_type,
            reason=reason,
        )
        self._callback(evt)

        # Simulate duplicate delivery (e.g., webhook retry)
        if random.random() < self.duplicate_event_prob:
            self._callback(evt)

    def _run_call_lifecycle(self, provider_call_id: str, cancel_flag: threading.Event):
        # 1. RINGING
        time.sleep(self.ring_duration_sec * 0.3)
        if cancel_flag.is_set():
            self._emit(provider_call_id, "CANCELLED", "Cancelled before ring")
            return

        ring_evt = ProviderEvent(
            event_id=f"evt-{uuid.uuid4().hex[:12]}",
            provider_call_id=provider_call_id,
            event_type="RINGING",
        )

        # Optionally hold RINGING to emit out-of-order after ANSWERED
        delay_ringing = random.random() < self.out_of_order_prob
        if not delay_ringing and self._callback:
            self._callback(ring_evt)
            if random.random() < self.duplicate_event_prob:
                self._callback(ring_evt)

        # 2. Wait ring duration
        time.sleep(self.ring_duration_sec * 0.7)
        if cancel_flag.is_set():
            self._emit(provider_call_id, "CANCELLED", "Cancelled while ringing")
            return

        # 3. Decide ANSWERED vs NO_ANSWER
        if random.random() > self.answer_rate:
            if delay_ringing and self._callback:
                self._callback(ring_evt)
            self._emit(provider_call_id, "FAILED", "NO_ANSWER")
            return

        self._emit(provider_call_id, "ANSWERED")

        # Flush delayed out-of-order RINGING event after ANSWERED
        if delay_ringing and self._callback:
            self._callback(ring_evt)

        # 4. Talk duration -> COMPLETED
        time.sleep(self.talk_duration_sec)
        self._emit(provider_call_id, "COMPLETED")
