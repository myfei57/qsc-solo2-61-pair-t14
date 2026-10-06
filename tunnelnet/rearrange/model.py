"""Plans, snapshots and receipts for atomic rearrangements."""

from __future__ import annotations

from dataclasses import dataclass, field


class RearrangeError(ValueError):
    """A rearrange plan failed validation or cannot be carried out."""


class RearrangeConflict(RearrangeError):
    """The plan was written against a generation that is no longer current."""


@dataclass(frozen=True)
class RearrangePlan:
    """One atomic rearrangement request.

    ``assignments`` maps partition -> cabin, ``bindings`` maps sensor ->
    partition.  Both are merged over the live state; the patrol route is
    rebuilt from the resulting mapping in the same commit.  ``request_id``
    is the idempotency key and ``base_generation`` an optional optimistic
    concurrency guard (0 skips the check).
    """

    request_id: str
    assignments: dict[str, str] = field(default_factory=dict)
    bindings: dict[str, str] = field(default_factory=dict)
    base_generation: int = 0
    at: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "assignments": dict(self.assignments),
            "bindings": dict(self.bindings),
            "base_generation": self.base_generation,
            "at": self.at,
        }


@dataclass(frozen=True)
class GenerationView:
    """An immutable, consistent read of the live rearranged state."""

    generation: int
    mapping: dict[str, str]
    bindings: dict[str, str]
    route: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "generation": self.generation,
            "mapping": dict(self.mapping),
            "bindings": dict(self.bindings),
            "route": list(self.route),
        }


@dataclass(frozen=True)
class GenerationSnapshot:
    """The confirmed content of one generation, kept for rollback and review."""

    generation: int
    mapping: dict[str, str]
    bindings: dict[str, str]
    route: tuple[str, ...]
    request_id: str
    at: str

    def matches(self, view: GenerationView) -> bool:
        """Whether the live view still equals this confirmed snapshot."""

        return (
            self.mapping == view.mapping
            and self.bindings == view.bindings
            and self.route == view.route
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "generation": self.generation,
            "mapping": dict(self.mapping),
            "bindings": dict(self.bindings),
            "route": list(self.route),
            "request_id": self.request_id,
            "at": self.at,
        }


@dataclass(frozen=True)
class Receipt:
    """The recorded outcome of one rearrange request.

    Status is one of ``committed``, ``rolled_back``, ``reverted``,
    ``adopted`` or ``unchanged``.  ``replayed`` marks a receipt that was
    served from the idempotency ledger instead of being applied again.
    """

    request_id: str
    status: str
    generation: int
    detail: str
    at: str
    replayed: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "status": self.status,
            "generation": self.generation,
            "detail": self.detail,
            "at": self.at,
            "replayed": self.replayed,
        }
