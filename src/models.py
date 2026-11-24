from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time


class AgentState(str, Enum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


class CallState(str, Enum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


CALL_STATE_RANK = {
    CallState.QUEUED: 0,
    CallState.RESERVED: 1,
    CallState.INITIATED: 2,
    CallState.RINGING: 3,
    CallState.ANSWERED: 4,
    CallState.CONNECTED: 5,
    CallState.COMPLETED: 6,
    CallState.FAILED: 6,
    CallState.CANCELLED: 6,
}

TERMINAL_CALL_STATES = {
    CallState.COMPLETED,
    CallState.FAILED,
    CallState.CANCELLED,
}


@dataclass
class Agent:
    agent_id: str
    name: str
    state: AgentState = AgentState.AVAILABLE
    current_call_id: Optional[str] = None
    version: int = 1
    updated_at: float = field(default_factory=time.time)


@dataclass
class Borrower:
    borrower_id: str
    phone_number: str
    name: str
    status: str = "PENDING"  # PENDING, RESERVED, IN_PROGRESS, DONE, RETRY
    attempts: int = 0
    max_attempts: int = 3
    version: int = 1
    updated_at: float = field(default_factory=time.time)


@dataclass
class Call:
    call_id: str
    borrower_id: str
    mode: str  # PROGRESSIVE or PREDICTIVE
    agent_id: Optional[str] = None
    provider_call_id: Optional[str] = None
    provider_name: Optional[str] = None
    state: CallState = CallState.QUEUED
    worker_id: Optional[str] = None
    version: int = 1
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    answered_at: Optional[float] = None
    connected_at: Optional[float] = None
    ended_at: Optional[float] = None
    abandoned: bool = False
    failure_reason: Optional[str] = None


@dataclass
class ProviderEvent:
    event_id: str
    provider_call_id: str
    event_type: str  # INITIATED, RINGING, ANSWERED, COMPLETED, FAILED
    timestamp: float = field(default_factory=time.time)
    reason: Optional[str] = None
