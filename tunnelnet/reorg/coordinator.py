"""Serialises every zone reorganisation across the three downstream readers.

The coordinator is the *only* component allowed to move the partition table,
the environment alarm router and the patrol route as a set.  It owns:

* a single re-entrant lock that makes concurrent reorganisations serial;
* the append only :class:`ReorgLedger` of layout versions and requests;
* the current layout pointer and the last confirmed (rollback anchor) layout;
* the audit trail of every lifecycle transition.

Timing decision (fixed): a submitted reorg is staged first and becomes live at
the current patrol round boundary (:meth:`close_round`); with no active round
it activates immediately.  Alarms already being handled keep the layout they
were born on; only alarms born after activation see the new routing.

Rollback decision (fixed): rollback restores the last *confirmed* layout and
does so by appending a compensating version, never by deleting history.
"""

from __future__ import annotations

import threading
import uuid
from typing import Callable

from tunnelnet.audit import AuditRegistry, Record
from tunnelnet.cabin import Checkpoint, PartitionTable, Registry as CabinRegistry
from tunnelnet.drain import utc_now
from tunnelnet.store import Store

from .ledger import ReorgLedger
from .manifest import validate_manifest
from .model import (
    EFFECT_IMMEDIATE,
    EFFECT_ROUND_BOUNDARY,
    ORIGIN_BOOTSTRAP,
    ORIGIN_HOTFIX,
    ORIGIN_REORG,
    ORIGIN_ROLLBACK,
    STATUS_ACTIVE,
    STATUS_ARMED,
    STATUS_CANCELLED,
    STATUS_CONFIRMED,
    STATUS_FAILED,
    STATUS_ROLLED_BACK,
    ReorgRequest,
    ZoneLayout,
    fingerprint,
)

Clock = Callable[[], str]


class ReorgConflict(ValueError):
    """Raised when a concurrent or stale base version collides (HTTP 409)."""


class ReorgCoordinator:
    """Atomic, idempotent and audited zone layout transitions."""

    def __init__(
        self,
        store: Store,
        cabins: CabinRegistry,
        partitions: PartitionTable,
        env: object,
        patrol: object,
        audit: AuditRegistry,
        clock: Clock = utc_now,
    ) -> None:
        self.cabins = cabins
        self.partitions = partitions
        self.env = env
        self.patrol = patrol
        self.audit = audit
        self._clock = clock
        self._lock = threading.RLock()
        self.ledger = ReorgLedger(store)
        self._layouts: dict[int, ZoneLayout] = {}
        self._current_version = 0
        self._requests: dict[str, ReorgRequest] = {}

    # ------------------------------------------------------------------ bootstrap

    def preload(self) -> int:
        """Restore ledger pointers before the boot seed mutates projections.

        Returns the persisted current version (0 on a fresh store).  The seed
        that follows then sees the correct layout version, so alarms it raises
        are stamped with it.
        """

        with self._lock:
            if self._layouts:
                return self._current_version
            for saved in self.ledger.layouts():
                self._layouts[saved.version] = saved
            for request in self.ledger.requests():
                self._requests[request.request_id] = request
            persisted = self.ledger.current_version()
            if persisted and persisted in self._layouts:
                self._current_version = persisted
            return self._current_version

    def bootstrap(self, at: str = "") -> ZoneLayout:
        """Capture the running wiring as confirmed layout v1 (idempotent)."""

        with self._lock:
            if self._current_version and self._current_version in self._layouts:
                layout = self._layouts[self._current_version]
                self._project(layout)
                return layout
            stamp = at or self._clock()
            layout = ZoneLayout(
                version=1,
                partition_routes=dict(self.partitions.mapping()),
                patrol_route=self.patrol.route_ids(),
                sensor_partitions=dict(self.env.sensor_partitions()),
                based_on=0,
                created_at=stamp,
                activated_at=stamp,
                confirmed=True,
                confirmed_at=stamp,
                origin=ORIGIN_BOOTSTRAP,
                fingerprint="",
            )
            layout = self._with_fingerprint(layout)
            self._layouts[1] = layout
            self._current_version = 1
            self._project(layout)
            self.ledger.save_layout(layout)
            self.ledger.set_current(1)
            return layout

    # --------------------------------------------------------------------- reads

    def current_version(self) -> int:
        with self._lock:
            return self._current_version

    def current_layout(self) -> ZoneLayout:
        with self._lock:
            return self._layouts[self._current_version]

    def layout(self, version: int) -> ZoneLayout | None:
        with self._lock:
            return self._layouts.get(version)

    def versions(self) -> list[ZoneLayout]:
        with self._lock:
            return [self._layouts[key] for key in sorted(self._layouts)]

    def requests(self) -> list[ReorgRequest]:
        with self._lock:
            return sorted(self._requests.values(), key=lambda item: (item.created_at, item.request_id))

    def request(self, request_id: str) -> ReorgRequest | None:
        with self._lock:
            return self._requests.get(request_id)

    def pending_request(self) -> ReorgRequest | None:
        """The staged (``armed``) or not yet confirmed (``active``) reorg."""

        with self._lock:
            for item in self._requests.values():
                if item.status in (STATUS_ARMED, STATUS_ACTIVE):
                    return item
            return None

    def last_confirmed(self) -> ZoneLayout | None:
        with self._lock:
            confirmed = [layout for layout in self._layouts.values() if layout.confirmed]
            if not confirmed:
                return None
            # A compensating rollback version inherits the anchor it restored,
            # so the "last confirmed snapshot" remains the target content.
            return max(confirmed, key=lambda layout: layout.anchor_version())

    def layout_effective_at(self, at: str) -> ZoneLayout | None:
        """Layout routing in force at ``at`` (boundaries are inclusive)."""

        with self._lock:
            candidates = [
                layout
                for layout in self._layouts.values()
                if layout.activated_at and layout.activated_at <= at
            ]
            if not candidates:
                return None
            return max(candidates, key=lambda layout: (layout.activated_at, layout.version))

    def manifest_snapshot(self) -> dict[str, object]:
        with self._lock:
            layout = self.current_layout()
            snapshot = layout.as_manifest()
            snapshot["version"] = layout.version
            snapshot["fingerprint"] = layout.fingerprint
            return snapshot

    def state(self) -> dict[str, object]:
        with self._lock:
            pending = self.pending_request()
            return {
                "current_version": self._current_version,
                "pending": None if pending is None else pending.request_id,
                "pending_status": "" if pending is None else pending.status,
                "versions": len(self._layouts),
                "requests": len(self._requests),
                "active_round": self.patrol.round_number(),
            }

    # ------------------------------------------------------------------ submit

    def submit(
        self,
        request_id: str,
        raw_manifest: dict[str, object],
        *,
        effective: str = EFFECT_ROUND_BOUNDARY,
        reason: str = "",
        base_version: int = 0,
        at: str = "",
    ) -> tuple[ReorgRequest, bool]:
        """Stage or immediately activate one reorg. Returns ``(request, replay)``.

        ``request_id`` is the idempotency key: replaying a finished or pending
        request returns the stored record untouched.
        """

        if not request_id:
            raise ValueError("request_id is required")
        stamp = at or self._clock()
        with self._lock:
            existing = self._requests.get(request_id)
            if existing is not None:
                return existing, True

            current = self.current_layout()
            if base_version and base_version != current.version:
                raise ReorgConflict(
                    f"base version {base_version} is stale; current version is {current.version}"
                )
            pending = self.pending_request()
            if pending is not None:
                raise ReorgConflict(
                    f"reorg {pending.request_id} is {pending.status}; confirm or roll it back first"
                )

            manifest = validate_manifest(raw_manifest, self.cabins)
            partition_routes: dict[str, str] = manifest["partitions"]  # type: ignore[assignment]
            patrol_route: list[str] = manifest["route"]  # type: ignore[assignment]
            sensor_partitions: dict[str, str] = manifest["sensors"]  # type: ignore[assignment]
            digest = fingerprint(partition_routes, patrol_route, sensor_partitions)
            if digest == current.fingerprint:
                raise ValueError("manifest is identical to the current layout")

            candidate_version = max(self._layouts) + 1 if self._layouts else 1
            layout = ZoneLayout(
                version=candidate_version,
                partition_routes=dict(partition_routes),
                patrol_route=list(patrol_route),
                sensor_partitions=dict(sensor_partitions),
                based_on=current.version,
                created_at=stamp,
                origin=ORIGIN_REORG,
                fingerprint=digest,
            )
            request = ReorgRequest(
                request_id=request_id,
                status=STATUS_ARMED,
                manifest=manifest,
                manifest_fingerprint=digest,
                base_version=current.version,
                candidate_version=candidate_version,
                effective=effective,
                origin=ORIGIN_REORG,
                reason=reason,
                created_at=stamp,
            )
            self._layouts[candidate_version] = layout
            self._requests[request_id] = request
            self.ledger.save_layout(layout)
            self.ledger.save_request(request)

            if effective == EFFECT_IMMEDIATE or not self.patrol.round_active():
                request = self._activate(request_id, stamp)
            else:
                self._audit("reorg_armed", request_id, stamp, current.version, candidate_version)
            return request, False

    # ----------------------------------------------------------- round boundary

    def close_round(self, at: str = "") -> ReorgRequest | None:
        """End the current patrol round; an armed reorg activates atomically.

        The old round finishes on the old route and the new round opens on the
        new one, so a single round is never inspected against two layouts.
        """

        stamp = at or self._clock()
        with self._lock:
            armed = next(
                (item for item in self._requests.values() if item.status == STATUS_ARMED),
                None,
            )
            if armed is None:
                self.patrol.end_round(None)
                self._audit("patrol_round_closed", f"round-{self.patrol.round_number() - 1}", stamp)
                return None
            finished = self.patrol.round_number()
            activated = self._activate(armed.request_id, stamp, round_finished=finished)
            return activated

    def activate_now(self, request_id: str, at: str = "") -> ReorgRequest:
        """Operator override: activate a staged reorg before round end."""

        with self._lock:
            return self._activate(request_id, at or self._clock())

    # -------------------------------------------------------------- confirm

    def confirm(self, request_id: str, at: str = "") -> ReorgRequest:
        """Mark the live reorg version verified; it becomes the rollback anchor."""

        stamp = at or self._clock()
        with self._lock:
            request = self._require_request(request_id)
            if request.status == STATUS_CONFIRMED:
                return request
            if request.status == STATUS_ARMED:
                raise ValueError("reorg is staged and has not activated yet")
            if request.status != STATUS_ACTIVE:
                raise ValueError(f"reorg is {request.status} and cannot be confirmed")
            layout = self._layouts[request.candidate_version]
            layout = self._with_fingerprint(
                layout, confirmed=True, confirmed_at=stamp  # type: ignore[arg-type]
            )
            self._layouts[layout.version] = layout
            request = request.with_status(status=STATUS_CONFIRMED, confirmed_at=stamp, finished_at=stamp)
            self._requests[request_id] = request
            self.ledger.save_layout(layout)
            self.ledger.save_request(request)
            self._audit("reorg_confirmed", request_id, stamp, restored=layout.version)
            return request

    # --------------------------------------------------------------- rollback

    def rollback(self, request_id: str, at: str = "") -> ReorgRequest:
        """Cancel a staged reorg or restore the last confirmed layout."""

        stamp = at or self._clock()
        with self._lock:
            request = self._require_request(request_id)
            if request.status == STATUS_ARMED:
                cancelled = request.with_status(
                    status=STATUS_CANCELLED, finished_at=stamp, restored_version=self._current_version
                )
                self._requests[request_id] = cancelled
                self.ledger.save_request(cancelled)
                self._audit("reorg_cancelled", request_id, stamp, restored=self._current_version)
                return cancelled

            if request.status == STATUS_CONFIRMED:
                raise ReorgConflict("reorg is already confirmed; rollback target is the previous snapshot")
            if request.status != STATUS_ACTIVE:
                raise ValueError(f"reorg is {request.status} and cannot be rolled back")

            target = self.last_confirmed()
            if target is None or target.anchor_version() >= self._current_version:
                raise ReorgConflict("no earlier confirmed snapshot exists")
            anchor = target.anchor_version()
            restored = self._compensate(target, request_id, stamp)
            rolled = request.with_status(
                status=STATUS_ROLLED_BACK,
                finished_at=stamp,
                restored_version=anchor,
            )
            self._requests[request_id] = rolled
            self.ledger.save_request(rolled)
            self._audit("reorg_rolled_back", request_id, stamp, restored=restored.version)
            return rolled

    # ------------------------------------------------------- legacy hot edit

    def record_hotfix(self, partition_id: str, cabin_id: str, at: str = "") -> ZoneLayout:
        """Version the legacy single partition edit so alarm pinning still works.

        Unlike a full reorg the patrol route is not rewritten; the layout is
        stamped ``hotfix`` and auto-confirmed so rollback anchors keep moving
        with reality.  A no-op edit returns the current layout unchanged.
        """

        stamp = at or self._clock()
        with self._lock:
            current = self.current_layout()
            if current.cabin_of_partition(partition_id) == cabin_id:
                return current
            revision = self.partitions.repartition(partition_id, cabin_id)
            routes = dict(self.partitions.mapping())
            version = max(self._layouts) + 1
            layout = self._with_fingerprint(
                ZoneLayout(
                    version=version,
                    partition_routes=routes,
                    patrol_route=list(current.patrol_route),
                    sensor_partitions=dict(current.sensor_partitions),
                    based_on=current.version,
                    created_at=stamp,
                    activated_at=stamp,
                    confirmed=True,
                    confirmed_at=stamp,
                    origin=ORIGIN_HOTFIX,
                )
            )
            request = ReorgRequest(
                request_id=f"hotfix-{version}-{partition_id}-{uuid.uuid4().hex[:8]}",
                status=STATUS_CONFIRMED,
                manifest=layout.as_manifest(),
                manifest_fingerprint=layout.fingerprint,
                base_version=current.version,
                candidate_version=version,
                effective=EFFECT_IMMEDIATE,
                origin=ORIGIN_HOTFIX,
                reason=f"legacy repartition {partition_id} -> {cabin_id} (revision {revision})",
                created_at=stamp,
                activated_at=stamp,
                confirmed_at=stamp,
                finished_at=stamp,
            )
            self._layouts[version] = layout
            self._requests[request.request_id] = request
            self._current_version = version
            self._project(layout)
            self.ledger.save_layout(layout)
            self.ledger.save_request(request)
            self.ledger.set_current(version)
            self._audit("partition_hotfix", request.request_id, stamp, restored=version)
            return layout

    # ------------------------------------------------------- alarm re-check

    def review_alarm(self, alarm: dict[str, object]) -> dict[str, object]:
        """Re-check an alarm against the layout it was born on.

        The alarm's birth partition, cabin and version are frozen on it and
        never recomputed; this projection additionally shows whether the
        *current* routing would send the same alarm elsewhere now.
        """

        with self._lock:
            birth_version = int(alarm.get("birth_version", 0) or 0)
            birth_partition = str(alarm.get("birth_partition", ""))
            birth_cabin = str(alarm.get("birth_cabin", ""))
            at = str(alarm.get("at", ""))
            birth_layout = self._layouts.get(birth_version)
            birth_resolved = ""
            birth_intact = False
            if birth_layout is not None:
                birth_resolved = birth_layout.cabin_of_partition(birth_partition)
                birth_intact = birth_resolved == birth_cabin and bool(birth_cabin)
            timed = self.layout_effective_at(at) if at else None
            current = self.current_layout()
            current_cabin = current.cabin_of_partition(birth_partition)
            return {
                "sensor_id": str(alarm.get("sensor_id", "")),
                "at": at,
                "birth_version": birth_version,
                "birth_partition": birth_partition,
                "birth_cabin": birth_cabin,
                "birth_layout_cabin": birth_resolved,
                "birth_attribution_intact": birth_intact,
                "version_effective_at_alarm": 0 if timed is None else timed.version,
                "current_version": current.version,
                "current_cabin": current_cabin,
                "routing_changed_since_birth": current_cabin != birth_cabin,
            }

    # ---------------------------------------------------------------- internal

    def _activate(
        self,
        request_id: str,
        stamp: str,
        round_finished: int = 0,
    ) -> ReorgRequest:
        request = self._require_request(request_id)
        if request.status == STATUS_ACTIVE:
            return request
        if request.status == STATUS_CONFIRMED:
            return request
        if request.status != STATUS_ARMED:
            raise ValueError(f"reorg is {request.status} and cannot be activated")
        layout = self._layouts[request.candidate_version]
        activated = self._with_fingerprint(layout, activated_at=stamp)  # type: ignore[arg-type]

        backup = self._snapshot_projections()
        archived_round = 0
        try:
            if round_finished:
                # Archive the old round on its actual route; the next round
                # opens on the new route together with the other projections.
                archived = self.patrol.archive_round()
                archived_round = archived.number
            self._commit_projections(activated)
        except Exception:
            # Restoring the captured snapshot must not itself be interrupted
            # by the same downstream fault that aborted the commit; each
            # projection restore is therefore isolated.
            self._safe_restore(backup, archived_round)
            failed = request.with_status(status=STATUS_FAILED, finished_at=stamp, error="activation rolled back")
            self._requests[request_id] = failed
            self.ledger.save_request(failed)
            self._audit("reorg_failed", request_id, stamp)
            raise

        self._layouts[activated.version] = activated
        self._current_version = activated.version
        self.ledger.save_layout(activated)
        self.ledger.set_current(activated.version)
        request = request.with_status(status=STATUS_ACTIVE, activated_at=stamp)
        self._requests[request_id] = request
        self.ledger.save_request(request)
        self._audit(
            "reorg_activated",
            request_id,
            stamp,
            request.base_version,
            activated.version,
            archived_round,
        )
        return request

    def _compensate(self, target: ZoneLayout, source_request: str, stamp: str) -> ZoneLayout:
        version = max(self._layouts) + 1
        restored = self._with_fingerprint(
            ZoneLayout(
                version=version,
                partition_routes=dict(target.partition_routes),
                patrol_route=list(target.patrol_route),
                sensor_partitions=dict(target.sensor_partitions),
                based_on=self._current_version,
                created_at=stamp,
                activated_at=stamp,
                confirmed=True,
                confirmed_at=stamp,
                origin=ORIGIN_ROLLBACK,
                restores_version=target.anchor_version(),
            )
        )
        backup = self._snapshot_projections()
        try:
            self._commit_projections(restored)
        except Exception:
            self._restore_projections(backup)
            self._audit("reorg_failed", f"rollback:{source_request}", stamp)
            raise
        self._layouts[version] = restored
        self._current_version = version
        self.ledger.save_layout(restored)
        self.ledger.set_current(version)
        return restored

    def _snapshot_projections(self) -> dict[str, object]:
        return {
            "mapping": dict(self.partitions.mapping()),
            "revision": self.partitions.revision(),
            "sensor_partitions": dict(self.env.sensor_partitions()),
            "env_version": self.env.layout_version(),
            "route_ids": self.patrol.route_ids(),
        }

    def _commit_projections(self, layout: ZoneLayout) -> None:
        """Apply the three projections; all or nothing."""

        self.partitions.replace_mapping(layout.partition_routes)
        self.env.apply_layout(
            version=layout.version,
            partition_routes=layout.partition_routes,
            sensor_partitions=layout.sensor_partitions,
        )
        new_route = [Checkpoint(cabin_id=cid, order=i) for i, cid in enumerate(layout.patrol_route)]
        if not self.patrol.round_active():
            # The coordinator archived the previous round at its boundary; the
            # next round opens on the new route, as part of this same commit.
            self.patrol.open_round(new_route)
        else:
            self.patrol.replace_route(new_route)

    def _restore_projections(self, backup: dict[str, object]) -> None:
        self.partitions.restore_mapping(dict(backup["mapping"]), int(backup["revision"]))  # type: ignore[arg-type]
        self.env.restore_view(
            version=int(backup["env_version"]),  # type: ignore[arg-type]
            sensor_partitions=dict(backup["sensor_partitions"]),  # type: ignore[arg-type]
        )
        old_route = [
            Checkpoint(cabin_id=cid, order=i)
            for i, cid in enumerate(list(backup["route_ids"]))  # type: ignore[arg-type]
        ]
        self.patrol.replace_route(old_route)

    def _safe_restore(self, backup: dict[str, object], archived_round: int) -> None:
        """Restore a failed commit's snapshot, tolerating secondary faults.

        If the commit archived a round before failing, the archived round is
        also reopened on its old route so the patrol service is not left with
        no active round.
        """

        errors: list[str] = []
        try:
            self.partitions.restore_mapping(
                dict(backup["mapping"]), int(backup["revision"])  # type: ignore[arg-type]
            )
        except Exception as exc:  # noqa: BLE001 - rollback must complete
            errors.append(f"partitions: {exc}")
        try:
            self.env.restore_view(
                version=int(backup["env_version"]),  # type: ignore[arg-type]
                sensor_partitions=dict(backup["sensor_partitions"]),  # type: ignore[arg-type]
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"environment: {exc}")
        try:
            old_route = [
                Checkpoint(cabin_id=cid, order=i)
                for i, cid in enumerate(list(backup["route_ids"]))  # type: ignore[arg-type]
            ]
            if archived_round:
                self.patrol.reopen_archived_round(old_route)
            else:
                self.patrol.replace_route(old_route)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"patrol: {exc}")
        if errors:
            self._audit("reorg_restore_degraded", ";".join(errors), self._clock())

    def _project(self, layout: ZoneLayout) -> None:
        """Rebuild in memory projections without touching the ledger."""

        self.partitions.restore_mapping(dict(layout.partition_routes), self.partitions.revision())
        self.env.apply_layout(
            version=layout.version,
            partition_routes=layout.partition_routes,
            sensor_partitions=layout.sensor_partitions,
        )
        new_route = [Checkpoint(cabin_id=cid, order=i) for i, cid in enumerate(layout.patrol_route)]
        self.patrol.replace_route(new_route)

    def _with_fingerprint(self, layout: ZoneLayout, **changes: object) -> ZoneLayout:
        from dataclasses import replace

        digest = fingerprint(layout.partition_routes, layout.patrol_route, layout.sensor_partitions)
        return replace(layout, fingerprint=digest, **changes)

    def _require_request(self, request_id: str) -> ReorgRequest:
        request = self._requests.get(request_id)
        if request is None:
            raise ValueError(f"reorg request {request_id} not found")
        return request

    def _audit(
        self,
        event: str,
        ref: str,
        at: str,
        from_version: int = 0,
        to_version: int = 0,
        round_closed: int = 0,
        *,
        restored: int = 0,
    ) -> None:
        """Append a main audit event and its ``_ref`` twin (console convention).

        The main event is filterable via ``/api/audit/query?event=``; the twin
        preserves the reorg pointer.  Full before/after content lives in the
        durable layout versions, which are the authoritative audit document.
        """

        self.audit.append(Record(id=uuid.uuid4().hex, event=event, at=at))
        self.audit.append(Record(id=uuid.uuid4().hex, event=f"{event}_ref", at=at))
