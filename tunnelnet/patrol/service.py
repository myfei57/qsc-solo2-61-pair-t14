"""Patrol service built from the live cabin registry.

A *round* is one pass over the route then in force.  The reorg coordinator
finishes the active round on the old route and opens the next round on the new
route, so a reorg never changes a route mid-round.  Until
:meth:`end_round` is called the first round stays active (preserving the
legacy continuous check-in behaviour).
"""

from __future__ import annotations

import threading

from tunnelnet.cabin import Checkpoint, Registry, checkpoints

from .model import CheckIn, PatrolSummary, Round
from .report import PatrolState
from .schedule import overdue_cabins, progress_ratio


class PatrolService:
    """Keeps the patrol route, rounds, check-ins and the overdue set."""

    def __init__(self, cabins: Registry) -> None:
        self._cabins = cabins
        self._lock = threading.RLock()
        self._route: list[Checkpoint] = checkpoints(cabins)
        self._round = 1
        self._active = True
        self._checks: list[CheckIn] = []
        self._rounds: list[Round] = []
        self._due: dict[str, str] = {}

    def route(self) -> list[Checkpoint]:
        with self._lock:
            return list(self._route)

    def rebuild_route(self) -> list[Checkpoint]:
        """Rebuild the route from the cabins that are active right now."""

        with self._lock:
            self._route = checkpoints(self._cabins)
            return list(self._route)

    def replace_route(self, route: list[Checkpoint]) -> list[Checkpoint]:
        """Install a route produced by an atomic reorg."""

        with self._lock:
            self._route = list(route)
            return list(self._route)

    def check_in(self, cabin_id: str, at: str) -> CheckIn:
        with self._lock:
            # The first round keeps the legacy continuous check-in behaviour and
            # is not stamped; later rounds are tagged so archived rounds keep
            # visits distinguishable across reorg boundaries.
            stamped_round = self._round if self._round > 1 else 0
            check = CheckIn(cabin_id=cabin_id, at=at, round=stamped_round)
            self._checks.append(check)
        return check

    def check_ins(self) -> list[CheckIn]:
        with self._lock:
            return list(self._checks)

    def round_active(self) -> bool:
        with self._lock:
            return self._active

    def round_number(self) -> int:
        with self._lock:
            return self._round

    def rounds(self) -> list[Round]:
        with self._lock:
            return list(self._rounds)

    def archive_round(self, at: str = "") -> Round:
        """Archive the active round without opening a new route yet.

        Used by the reorg coordinator, which installs the next route
        atomically as part of the same committed layout.
        """

        with self._lock:
            archived = Round(
                number=self._round,
                route=[checkpoint.cabin_id for checkpoint in self._route],
                check_ins=list(self._checks),
                ended_at=at,
            )
            self._rounds.append(archived)
            self._round += 1
            self._checks = []
            self._active = False
            return archived

    def open_round(self, route: list[Checkpoint] | None = None) -> int:
        """Open the next round on ``route`` (or a freshly rebuilt route)."""

        with self._lock:
            self._route = list(route) if route is not None else checkpoints(self._cabins)
            self._active = True
            return self._round

    def reopen_archived_round(self, route: list[Checkpoint]) -> None:
        """Undo an :meth:`archive_round` when the accompanying commit failed.

        The most recently archived round is restored as the active round, its
        check-ins put back, so the operator continues the interrupted round on
        its original route.
        """

        with self._lock:
            if not self._rounds:
                self._route = list(route)
                self._active = True
                return
            last = self._rounds.pop()
            self._round = last.number
            self._checks = list(last.check_ins)
            self._route = list(route)
            self._active = True

    def end_round(self, next_route: list[Checkpoint] | None = None, at: str = "") -> Round:
        """Archive the active round and open the next one.

        ``next_route`` is the route the reorg coordinator has prepared; the new
        round's check-in ledger starts empty.
        """

        with self._lock:
            archived = self.archive_round(at)
            if next_route is not None:
                self._route = list(next_route)
            else:
                self._route = checkpoints(self._cabins)
            self._active = True
            return archived

    def missed(self) -> list[str]:
        visited = {check.cabin_id for check in self.check_ins()}
        return [checkpoint.cabin_id for checkpoint in self.route() if checkpoint.cabin_id not in visited]

    def route_ids(self) -> list[str]:
        return [checkpoint.cabin_id for checkpoint in self.route()]

    def checked_cabin_ids(self) -> list[str]:
        return [check.cabin_id for check in self.check_ins()]

    def set_due(self, cabin_id: str, due: str) -> str:
        with self._lock:
            self._due[cabin_id] = due
            return self._due[cabin_id]

    def due(self, cabin_id: str) -> str:
        with self._lock:
            return self._due.get(cabin_id, "")

    def overdue(self, now: str) -> list[str]:
        return overdue_cabins(self.route(), self.check_ins(), dict(self._due), now)

    def progress(self) -> float:
        return progress_ratio(len(self._route), self.check_ins())

    def route_len(self) -> int:
        return len(self.route())

    def checked_count(self) -> int:
        return len(self.check_ins())

    def summary(self) -> PatrolSummary:
        return PatrolSummary(route=self.route_len(), checked=self.checked_count(), missed=len(self.missed()))

    def sorted_route(self) -> list[str]:
        return sorted(self.route_ids())

    def state(self) -> PatrolState:
        missed = self.missed()
        return PatrolState(
            route=self.route_ids(),
            checked=self.checked_cabin_ids(),
            missed=missed,
            progress=self.progress(),
        )

    def describe(self) -> str:
        return (
            f"patrol round={self._round} route={self.route_len()} "
            f"checked={self.checked_count()} missed={len(self.missed())}"
        )
