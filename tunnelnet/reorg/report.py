"""Read only projections of the reorg ledger for the console."""

from __future__ import annotations

from dataclasses import dataclass, field

from .model import ReorgRequest, ZoneLayout


@dataclass(frozen=True)
class LedgerState:
    """Versions, request lifecycle rows and the current/confirmed pointers."""

    current_version: int
    confirmed_version: int
    versions: list[dict[str, object]] = field(default_factory=list)
    requests: list[dict[str, object]] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "current_version": self.current_version,
            "confirmed_version": self.confirmed_version,
            "versions": self.versions,
            "requests": self.requests,
        }


def ledger_state(
    layouts: list[ZoneLayout],
    requests: list[ReorgRequest],
    current_version: int,
    confirmed_version: int,
) -> LedgerState:
    return LedgerState(
        current_version=current_version,
        confirmed_version=confirmed_version,
        versions=[layout.as_dict() for layout in layouts],
        requests=[request.as_dict() for request in requests],
    )
