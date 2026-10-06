"""Immutable zone layout versions and the lifecycle of one reorg request.

A layout is the *whole* routing picture at one point in time:

* ``partition_routes`` maps a partition id to the cabin that owns it. The
  environment alarm router consults this table.
* ``patrol_route`` is the ordered list of cabin ids the inspection round walks.
* ``sensor_partitions`` maps every environment sensor onto a partition.

The three projections always change together (see
:mod:`tunnelnet.reorg.coordinator`); a layout is therefore the unit of
atomicity and the anchor that historical alarms are re-checked against.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

ORIGIN_BOOTSTRAP = "bootstrap"
ORIGIN_REORG = "reorg"
ORIGIN_HOTFIX = "hotfix"
ORIGIN_ROLLBACK = "rollback"

EFFECT_IMMEDIATE = "immediate"
EFFECT_ROUND_BOUNDARY = "round_boundary"

STATUS_ARMED = "armed"
STATUS_ACTIVE = "active"
STATUS_CONFIRMED = "confirmed"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"
STATUS_ROLLED_BACK = "rolled_back"

ACTIVE_STATUSES = (STATUS_ACTIVE, STATUS_CONFIRMED)


def canonical_payload(layout: dict[str, object]) -> bytes:
    """Stable byte rendering used to compare and fingerprint layouts."""

    return json.dumps(layout, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def fingerprint(partition_routes: dict[str, str], patrol_route: list[str],
                sensor_partitions: dict[str, str]) -> str:
    """Content hash of the three projections that must move together."""

    body = canonical_payload(
        {
            "partition_routes": dict(sorted(partition_routes.items())),
            "patrol_route": list(patrol_route),
            "sensor_partitions": dict(sorted(sensor_partitions.items())),
        }
    )
    return hashlib.sha256(body).hexdigest()


@dataclass(frozen=True)
class ZoneLayout:
    """One immutable, fully consistent routing version."""

    version: int
    partition_routes: dict[str, str] = field(default_factory=dict)
    patrol_route: list[str] = field(default_factory=list)
    sensor_partitions: dict[str, str] = field(default_factory=dict)
    based_on: int = 0
    created_at: str = ""
    activated_at: str = ""
    confirmed: bool = False
    confirmed_at: str = ""
    origin: str = ORIGIN_REORG
    fingerprint: str = ""
    # For a rollback compensating version this is the confirmed version whose
    # content was restored; the compensating version inherits that anchor and
    # is not itself a new confirmation point.
    restores_version: int = 0

    def cabin_of_partition(self, partition_id: str) -> str:
        return self.partition_routes.get(partition_id, "")

    def route_cabins(self) -> set[str]:
        return set(self.patrol_route)

    def anchor_version(self) -> int:
        """The confirmed snapshot this layout represents (its rollback anchor)."""

        return self.restores_version or self.version

    def as_manifest(self) -> dict[str, object]:
        """Projection used by ``/api/reorg/snapshot`` and submissions."""

        return {
            "partitions": dict(self.partition_routes),
            "route": list(self.patrol_route),
            "sensors": dict(self.sensor_partitions),
        }

    def as_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "partition_routes": dict(self.partition_routes),
            "patrol_route": list(self.patrol_route),
            "sensor_partitions": dict(self.sensor_partitions),
            "based_on": self.based_on,
            "created_at": self.created_at,
            "activated_at": self.activated_at,
            "confirmed": self.confirmed,
            "confirmed_at": self.confirmed_at,
            "origin": self.origin,
            "fingerprint": self.fingerprint,
            "restores_version": self.restores_version,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "ZoneLayout":
        return cls(
            version=int(payload["version"]),
            partition_routes={str(k): str(v) for k, v in dict(payload.get("partition_routes", {})).items()},
            patrol_route=[str(item) for item in list(payload.get("patrol_route", []))],
            sensor_partitions={str(k): str(v) for k, v in dict(payload.get("sensor_partitions", {})).items()},
            based_on=int(payload.get("based_on", 0) or 0),
            created_at=str(payload.get("created_at", "")),
            activated_at=str(payload.get("activated_at", "")),
            confirmed=bool(payload.get("confirmed", False)),
            confirmed_at=str(payload.get("confirmed_at", "")),
            origin=str(payload.get("origin", ORIGIN_REORG)),
            fingerprint=str(payload.get("fingerprint", "")),
            restores_version=int(payload.get("restores_version", 0) or 0),
        )


@dataclass(frozen=True)
class ReorgRequest:
    """The lifecycle record of one submitted reorganisation.

    ``request_id`` is the client supplied idempotency key: replaying a request
    with the same key returns this record instead of applying a second change.
    """

    request_id: str
    status: str
    manifest: dict[str, object] = field(default_factory=dict)
    manifest_fingerprint: str = ""
    base_version: int = 0
    candidate_version: int = 0
    effective: str = EFFECT_ROUND_BOUNDARY
    origin: str = ORIGIN_REORG
    reason: str = ""
    created_at: str = ""
    activated_at: str = ""
    confirmed_at: str = ""
    finished_at: str = ""
    error: str = ""
    restored_version: int = 0

    def is_terminal(self) -> bool:
        return self.status in (STATUS_CONFIRMED, STATUS_FAILED, STATUS_CANCELLED, STATUS_ROLLED_BACK)

    def as_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "status": self.status,
            "manifest": self.manifest,
            "manifest_fingerprint": self.manifest_fingerprint,
            "base_version": self.base_version,
            "candidate_version": self.candidate_version,
            "effective": self.effective,
            "origin": self.origin,
            "reason": self.reason,
            "created_at": self.created_at,
            "activated_at": self.activated_at,
            "confirmed_at": self.confirmed_at,
            "finished_at": self.finished_at,
            "error": self.error,
            "restored_version": self.restored_version,
        }

    def with_status(self, **changes: object) -> "ReorgRequest":
        """Return a copy with fields replaced (the record itself stays frozen)."""

        from dataclasses import replace

        return replace(self, **changes)

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "ReorgRequest":
        return cls(
            request_id=str(payload["request_id"]),
            status=str(payload.get("status", STATUS_ARMED)),
            manifest=dict(payload.get("manifest", {})),
            manifest_fingerprint=str(payload.get("manifest_fingerprint", "")),
            base_version=int(payload.get("base_version", 0) or 0),
            candidate_version=int(payload.get("candidate_version", 0) or 0),
            effective=str(payload.get("effective", EFFECT_ROUND_BOUNDARY)),
            origin=str(payload.get("origin", ORIGIN_REORG)),
            reason=str(payload.get("reason", "")),
            created_at=str(payload.get("created_at", "")),
            activated_at=str(payload.get("activated_at", "")),
            confirmed_at=str(payload.get("confirmed_at", "")),
            finished_at=str(payload.get("finished_at", "")),
            error=str(payload.get("error", "")),
            restored_version=int(payload.get("restored_version", 0) or 0),
        )
