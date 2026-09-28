"""领域错误：携带稳定错误码，拒绝类命令不产生事件。"""

from __future__ import annotations


class DomainError(Exception):
    code = "domain_error"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code


class ValidationError(DomainError):
    code = "validation_error"


class NotFound(DomainError):
    code = "not_found"


class InvalidState(DomainError):
    code = "invalid_state"


class PermissionDenied(DomainError):
    code = "permission_denied"


class IdempotencyKeyMismatch(DomainError):
    code = "idempotency_key_mismatch"


class CandidateIsolationConflict(DomainError):
    code = "candidate_isolation_conflict"


class DuplicateCandidate(DomainError):
    code = "duplicate_candidate"


class LockConflict(DomainError):
    code = "lock_conflict"


class BudgetExceeded(DomainError):
    code = "budget_exceeded"


class ScheduleConflict(DomainError):
    code = "schedule_conflict"
