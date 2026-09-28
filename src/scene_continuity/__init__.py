"""AI短剧镜头连续性工单领域契约与工单服务。"""

from .clock import Clock, ManualClock
from .contracts import ContractIssue, validate_event
from .errors import (
    BudgetExceeded,
    CandidateIsolationConflict,
    DomainError,
    DuplicateCandidate,
    InvalidState,
    LockConflict,
    NotFound,
    PermissionDenied,
    ScheduleConflict,
    ValidationError,
)
from .service import Command, ContinuityService, Receipt
from .store import EventStore, StoredEvent

__all__ = [
    "BudgetExceeded",
    "CandidateIsolationConflict",
    "Clock",
    "Command",
    "ContractIssue",
    "ContinuityService",
    "DomainError",
    "DuplicateCandidate",
    "EventStore",
    "InvalidState",
    "LockConflict",
    "ManualClock",
    "NotFound",
    "PermissionDenied",
    "Receipt",
    "ScheduleConflict",
    "StoredEvent",
    "ValidationError",
    "validate_event",
]
