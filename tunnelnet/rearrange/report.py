"""State projection of the rearrange coordinator for the console."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RearrangeState:
    """Generation counters and the drift flag shown on the dashboard."""

    generation: int
    confirmed: int
    ledger: int
    drifted: bool
    last_request: str

    def as_dict(self) -> dict[str, object]:
        return {
            "generation": self.generation,
            "confirmed": self.confirmed,
            "ledger": self.ledger,
            "drifted": self.drifted,
            "last_request": self.last_request,
        }
