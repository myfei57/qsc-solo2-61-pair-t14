"""Atomic rearrangement of the partition table, alarm routing and patrol route.

A rearrangement commits as one generation: the partition mapping, the
sensor -> partition bindings and the patrol route become visible together,
or not at all.  Two semantics are fixed here by decision:

* Effective time: a commit takes effect immediately.  Alarms raised
  afterwards route by the new generation; alarms raised before keep the
  attribution they were stamped with, so a rearrangement never rewrites
  the handling of an in-flight alarm.
* Rollback: the only restore point is the last confirmed snapshot.  A
  transaction that fails midway never confirmed anything, so rollback
  simply keeps the current confirmed snapshot (nothing is applied).  A
  committed rearrangement is undone by re-committing the previous
  confirmed snapshot as a new generation; history stays append-only.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable

from tunnelnet.audit import AuditRegistry, Record
from tunnelnet.cabin import Checkpoint, PartitionTable, Registry
from tunnelnet.cabin.report import CabinState
from tunnelnet.drain import utc_now
from tunnelnet.env import Alarm, Environment
from tunnelnet.patrol import PatrolService
from tunnelnet.store import Store

from .model import (
    GenerationSnapshot,
    GenerationView,
    RearrangeConflict,
    RearrangeError,
    RearrangePlan,
    Receipt,
)
from .report import RearrangeState

KIND = "rearrange"

_EVENTS = {
    "committed": "rearrange_committed",
    "reverted": "rearrange_reverted",
    "rolled_back": "rearrange_rolled_back",
}


def planned_route(mapping: dict[str, str], active: list[CabinState]) -> list[Checkpoint]:
    """Order the patrol route so mapped cabins follow the partition layout.

    Cabins referenced by the new mapping come first, ordered by partition
    then cabin id; every remaining active cabin keeps its id order behind
    them, so the route still covers the whole registry.
    """

    live = {cabin.id for cabin in active}
    ordered: list[str] = []
    for _, cabin_id in sorted(mapping.items()):
        if cabin_id in live and cabin_id not in ordered:
            ordered.append(cabin_id)
    ordered.extend(cabin.id for cabin in active if cabin.id not in ordered)
    return [Checkpoint(cabin_id=cabin_id, order=index) for index, cabin_id in enumerate(ordered)]


class RearrangeCoordinator:
    """Serializes and journals every rearrangement of the shared topology.

    One lock guards submit, revert, view reads and alarm stamping, so a
    downstream reader either sees the whole new generation or the whole
    old one, never a mix.  Every request carries an idempotency key; the
    ledger (mirrored into the store) makes retries safe, including across
    a restart.
    """

    def __init__(
        self,
        cabins: Registry,
        partitions: PartitionTable,
        env: Environment,
        patrol: PatrolService,
        store: Store,
        audit: AuditRegistry,
        clock: Callable[[], str] = utc_now,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self._cabins = cabins
        self._partitions = partitions
        self._env = env
        self._patrol = patrol
        self._store = store
        self._audit = audit
        self._clock = clock
        self._fault = fault_injector if fault_injector is not None else (lambda stage: None)
        self._lock = threading.RLock()
        self._generations: list[GenerationSnapshot] = []
        self._ledger: dict[str, Receipt] = {}

    # ------------------------------------------------------------------
    # writes

    def submit(self, plan: RearrangePlan) -> Receipt:
        """Apply one plan atomically, or roll back to the confirmed state."""

        with self._lock:
            known = self._known(plan.request_id)
            if known is not None:
                return self._replay(known)
            self._validate(plan)
            mapping = {**self._partitions.mapping(), **plan.assignments}
            bindings = {**self._env.bindings(), **plan.bindings}
            route = planned_route(mapping, self._cabins.active())
            return self._commit(mapping, bindings, route, plan.request_id, "committed", plan.as_dict())

    def revert(self, request_id: str = "") -> Receipt:
        """Re-commit the last confirmed snapshot before the current one."""

        with self._lock:
            if len(self._generations) < 2:
                raise RearrangeError("no earlier confirmed snapshot to revert to")
            receipt_id = request_id or f"revert-{uuid.uuid4().hex}"
            known = self._known(receipt_id)
            if known is not None:
                return self._replay(known)
            target = self._generations[-2]
            route = [
                Checkpoint(cabin_id=cabin_id, order=index)
                for index, cabin_id in enumerate(target.route)
            ]
            return self._commit(
                dict(target.mapping),
                dict(target.bindings),
                route,
                receipt_id,
                "reverted",
                {"revert_to": target.generation},
            )

    def adopt_current(self, request_id: str, at: str = "") -> Receipt:
        """Confirm the live state as a generation without changing anything.

        Used to pin the seeded baseline.  Adoption is a bootstrap act, not
        an operator rearrangement, so it stays in memory: it writes neither
        the store journal nor the audit trail.
        """

        with self._lock:
            known = self._ledger.get(request_id)
            if known is not None:
                return self._replay(known)
            stamp = at or self._clock()
            view = self._live_view()
            if self._generations and self._generations[-1].matches(view):
                return Receipt(request_id, "unchanged", view.generation, "", stamp)
            snapshot = GenerationSnapshot(
                generation=view.generation + 1,
                mapping=view.mapping,
                bindings=view.bindings,
                route=view.route,
                request_id=request_id,
                at=stamp,
            )
            self._generations.append(snapshot)
            receipt = Receipt(request_id, "adopted", snapshot.generation, "", stamp)
            self._ledger[request_id] = receipt
            return receipt

    # ------------------------------------------------------------------
    # reads

    def current_generation(self) -> int:
        with self._lock:
            return self._generation()

    def current_view(self) -> GenerationView:
        """One consistent read of the live generation for downstream consumers."""

        with self._lock:
            return self._live_view()

    def stamp_for(self, sensor_id: str) -> tuple[int, str, str]:
        """Generation, partition and cabin an alarm raised right now belongs to.

        Served under the commit lock so an alarm can never be stamped with
        a half-applied generation.
        """

        with self._lock:
            partition = self._env.partition_of(sensor_id)
            return (self._generation(), partition, self._partitions.cabin_of(partition))

    def generations(self) -> list[GenerationSnapshot]:
        with self._lock:
            return list(self._generations)

    def ledger(self) -> list[Receipt]:
        with self._lock:
            return list(self._ledger.values())

    def drifted(self) -> bool:
        """Whether the live state no longer equals the last confirmed snapshot."""

        with self._lock:
            return bool(self._generations) and not self._generations[-1].matches(self._live_view())

    def review_alarms(self, alarms: list[Alarm]) -> list[dict[str, object]]:
        """Re-check every alarm against the generation it was stamped with.

        The stamped attribution is never rewritten; ``verified`` resolves
        the alarm's sensor inside its own generation's snapshot so a review
        can confirm the stamp, and ``current`` shows where the same sensor
        would route under the live generation.
        """

        with self._lock:
            snapshots = {snapshot.generation: snapshot for snapshot in self._generations}
            view = self._live_view()
            reviews: list[dict[str, object]] = []
            for alarm in alarms:
                snapshot = snapshots.get(alarm.generation)
                verified = None
                if snapshot is not None:
                    partition = snapshot.bindings.get(alarm.sensor_id, "")
                    cabin = snapshot.mapping.get(partition, "")
                    verified = {
                        "partition": partition,
                        "cabin": cabin,
                        "consistent": partition == alarm.partition and cabin == alarm.cabin,
                    }
                current_partition = view.bindings.get(alarm.sensor_id, "")
                reviews.append(
                    {
                        "sensor_id": alarm.sensor_id,
                        "value": alarm.value,
                        "stamped": {
                            "generation": alarm.generation,
                            "partition": alarm.partition,
                            "cabin": alarm.cabin,
                        },
                        "verified": verified,
                        "current": {
                            "generation": view.generation,
                            "partition": current_partition,
                            "cabin": view.mapping.get(current_partition, ""),
                        },
                    }
                )
            return reviews

    def inconsistent_alarm_count(self) -> int:
        """Alarms whose stamp no snapshot can confirm, a downstream mismatch."""

        with self._lock:
            known = {snapshot.generation for snapshot in self._generations}
            count = 0
            for item in self.review_alarms(self._env.alarms()):
                stamped = item["stamped"]
                verified = item["verified"]
                if verified is not None and not verified["consistent"]:
                    count += 1
                elif verified is None and stamped["generation"] not in known and stamped["generation"] != 0:
                    count += 1
            return count

    def state(self) -> RearrangeState:
        with self._lock:
            return RearrangeState(
                generation=self._generation(),
                confirmed=len(self._generations),
                ledger=len(self._ledger),
                drifted=self.drifted(),
                last_request=self._generations[-1].request_id if self._generations else "",
            )

    def describe(self) -> str:
        state = self.state()
        return (
            f"rearrange generation={state.generation} confirmed={state.confirmed} "
            f"drifted={state.drifted}"
        )

    # ------------------------------------------------------------------
    # internals, always called with the lock held

    def _generation(self) -> int:
        return self._generations[-1].generation if self._generations else 0

    def _live_view(self) -> GenerationView:
        return GenerationView(
            generation=self._generation(),
            mapping=self._partitions.mapping(),
            bindings=self._env.bindings(),
            route=tuple(self._patrol.route_ids()),
        )

    def _validate(self, plan: RearrangePlan) -> None:
        if not plan.request_id:
            raise RearrangeError("request_id is required")
        if not plan.assignments and not plan.bindings:
            raise RearrangeError("plan changes nothing")
        for partition_id, cabin_id in plan.assignments.items():
            if not partition_id:
                raise RearrangeError("partition id is empty")
            cabin = self._cabins.find(cabin_id)
            if cabin is None or cabin.retired:
                raise RearrangeError(f"cabin {cabin_id} not found")
        for sensor_id, partition_id in plan.bindings.items():
            if not sensor_id:
                raise RearrangeError("sensor id is empty")
            if not partition_id:
                raise RearrangeError("partition id is empty")
        if plan.base_generation < 0:
            raise RearrangeError("base_generation must be non-negative")
        current = self._generation()
        if plan.base_generation and plan.base_generation != current:
            raise RearrangeConflict(
                f"base generation {plan.base_generation} does not match current {current}"
            )

    def _commit(
        self,
        mapping: dict[str, str],
        bindings: dict[str, str],
        route: list[Checkpoint],
        request_id: str,
        status: str,
        plan: dict[str, object],
    ) -> Receipt:
        previous = self._live_view()
        try:
            self._fault("partitions")
            self._partitions.replace_all(mapping)
            self._fault("routing")
            self._env.rebind_all(bindings)
            self._fault("patrol")
            self._patrol.install_route(route)
        except Exception as exc:
            self._restore(previous)
            receipt = Receipt(
                request_id,
                "rolled_back",
                self._generation(),
                f"{type(exc).__name__}: {exc}",
                self._clock(),
            )
            self._record(receipt, plan, None)
            return receipt
        snapshot = GenerationSnapshot(
            generation=self._generation() + 1,
            mapping=dict(mapping),
            bindings=dict(bindings),
            route=tuple(checkpoint.cabin_id for checkpoint in route),
            request_id=request_id,
            at=self._clock(),
        )
        self._generations.append(snapshot)
        receipt = Receipt(request_id, status, snapshot.generation, "", self._clock())
        self._record(receipt, plan, snapshot)
        return receipt

    def _restore(self, view: GenerationView) -> None:
        """Put every component back to the last confirmed content."""

        self._partitions.replace_all(view.mapping)
        self._env.rebind_all(view.bindings)
        self._patrol.install_route(
            [Checkpoint(cabin_id=cabin_id, order=index) for index, cabin_id in enumerate(view.route)]
        )

    def _record(self, receipt: Receipt, plan: dict[str, object], snapshot: GenerationSnapshot | None) -> None:
        self._ledger[receipt.request_id] = receipt
        self._store.save(
            KIND,
            receipt.request_id,
            {
                "request_id": receipt.request_id,
                "status": receipt.status,
                "generation": receipt.generation,
                "detail": receipt.detail,
                "at": receipt.at,
                "plan": plan,
                "snapshot": None if snapshot is None else snapshot.as_dict(),
            },
        )
        self._audit.append(
            Record(
                id=uuid.uuid4().hex,
                event=_EVENTS.get(receipt.status, "rearrange"),
                at=receipt.at,
            )
        )

    def _known(self, request_id: str) -> Receipt | None:
        if not request_id:
            return None
        receipt = self._ledger.get(request_id)
        if receipt is not None:
            return receipt
        journal = self._store.load(KIND, request_id)
        if journal is None:
            return None
        receipt = Receipt(
            request_id=request_id,
            status=str(journal.get("status", "")),
            generation=int(journal.get("generation", 0)),
            detail=str(journal.get("detail", "")),
            at=str(journal.get("at", "")),
        )
        self._ledger[request_id] = receipt
        return receipt

    @staticmethod
    def _replay(receipt: Receipt) -> Receipt:
        return Receipt(
            request_id=receipt.request_id,
            status=receipt.status,
            generation=receipt.generation,
            detail=receipt.detail,
            at=receipt.at,
            replayed=True,
        )
