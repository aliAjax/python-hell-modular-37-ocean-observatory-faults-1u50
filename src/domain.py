from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class DomainError(Exception):
    """Base error for domain failures."""


class ValidationError(DomainError):
    """Input does not satisfy a domain rule."""


class PermissionDenied(DomainError):
    """Actor is not allowed to perform the action."""


class NotFoundError(DomainError):
    """Requested record does not exist."""


class ConflictError(DomainError):
    """A version or uniqueness constraint was violated."""


class InvalidTransition(DomainError):
    """The requested state transition is not valid."""


class MergeValidationError(DomainError):
    """An offline batch was rejected; nothing from it is stored.

    errors is a list of {"index", "record_id", "field", "message"}
    locations so the submitter can fix and retry the same batch.
    """

    def __init__(self, batch_id, errors):
        self.batch_id = batch_id
        self.errors = errors
        super().__init__("offline merge rejected with %d error(s)" % len(errors))


class Role(str, Enum):
    viewer = "viewer"
    admin = "admin"
    operator = "operator"
    engineer = "engineer"
    field = "field"


@dataclass
class Actor:
    user_id: str
    role: str

    @classmethod
    def from_headers(cls, headers):
        user_id = headers.get("X-User-Id", "anonymous")
        role = headers.get("X-Role", "viewer")
        if role not in {item.value for item in Role}:
            raise PermissionDenied("unknown role: " + role)
        return cls(user_id=user_id, role=role)


@dataclass
class Entity:
    id: str
    kind: str
    status: str
    version: int
    data: Dict[str, Any]
    created_by: str
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row):
        return cls(
            id=row["id"],
            kind=row["kind"],
            status=row["status"],
            version=row["version"],
            data=row["data"],
            created_by=row["created_by"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
