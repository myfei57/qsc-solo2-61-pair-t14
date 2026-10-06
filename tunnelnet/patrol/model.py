"""Patrol check-ins, inspection rounds and the aggregate summary."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CheckIn:
    """One cabin visit recorded by a patrol operator."""

    cabin_id: str
    at: str
    round: int = 0

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {"cabin_id": self.cabin_id, "at": self.at}
        if self.round:
            payload["round"] = self.round
        return payload


@dataclass(frozen=True)
class Round:
    """One finished inspection round, archived with the route it walked."""

    number: int
    route: list[str] = field(default_factory=list)
    check_ins: list[CheckIn] = field(default_factory=list)
    ended_at: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "number": self.number,
            "route": list(self.route),
            "check_ins": [check.as_dict() for check in self.check_ins],
            "ended_at": self.ended_at,
        }


@dataclass(frozen=True)
class PatrolSummary:
    """Route length, visited count and missed count."""

    route: int
    checked: int
    missed: int

    def as_dict(self) -> dict[str, int]:
        return {"route": self.route, "checked": self.checked, "missed": self.missed}
