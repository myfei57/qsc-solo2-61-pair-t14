"""Validation of a candidate layout against the live cabin registry.

The manifest a client submits must be internally consistent: every patrol
stop has to resolve to an active cabin, the patrol route and the partition
routing have to cover the same set of active cabins, and every referenced
partition and sensor has to resolve.  Anything else is rejected before a
candidate version is built, so a bad submission can never become a partial
change.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tunnelnet.cabin import Registry as CabinRegistry


def normalise_manifest(payload: dict[str, object]) -> dict[str, object]:
    """Coerce a raw JSON manifest into validated typed collections."""

    partitions = payload.get("partitions", {})
    route = payload.get("route", [])
    sensors = payload.get("sensors", {})
    if not isinstance(partitions, dict):
        raise ValueError("manifest.partitions must be an object")
    if not isinstance(route, (list, tuple)):
        raise ValueError("manifest.route must be a list")
    if not isinstance(sensors, dict):
        raise ValueError("manifest.sensors must be an object")
    return {
        "partitions": {str(k): str(v) for k, v in partitions.items()},
        "route": [str(item) for item in route],
        "sensors": {str(k): str(v) for k, v in sensors.items()},
    }


def validate_manifest(
    manifest: dict[str, object],
    cabins: "CabinRegistry",
    known_partitions: set[str] | None = None,
) -> dict[str, object]:
    """Validate and return a normalised manifest or raise ``ValueError``."""

    normalised = normalise_manifest(manifest)
    partition_routes: dict[str, str] = normalised["partitions"]  # type: ignore[assignment]
    patrol_route: list[str] = normalised["route"]  # type: ignore[assignment]
    sensor_partitions: dict[str, str] = normalised["sensors"]  # type: ignore[assignment]

    if not partition_routes:
        raise ValueError("manifest must route at least one partition")
    if not patrol_route:
        raise ValueError("manifest must define a patrol route")
    if any(not key for key in partition_routes):
        raise ValueError("partition id is empty")
    if any(not value for value in partition_routes.values()):
        raise ValueError("partition cabin id is empty")
    if any(not cabin_id for cabin_id in patrol_route):
        raise ValueError("patrol route cabin id is empty")
    if any(not key or not value for key, value in sensor_partitions.items()):
        raise ValueError("sensor binding is empty")
    if len(set(patrol_route)) != len(patrol_route):
        raise ValueError("patrol route visits a cabin more than once")
    if len(set(partition_routes)) != len(partition_routes.values()):
        raise ValueError("two partitions route to the same cabin")

    active = {cabin.id for cabin in cabins.active()}
    unknown_route = [cabin_id for cabin_id in patrol_route if cabin_id not in active]
    if unknown_route:
        raise ValueError(f"patrol route references unknown or retired cabins: {','.join(sorted(unknown_route))}")

    routed_cabins = set(partition_routes.values())
    unknown_routed = [cabin_id for cabin_id in sorted(routed_cabins) if cabin_id not in active]
    if unknown_routed:
        raise ValueError(f"partitions route to unknown or retired cabins: {','.join(unknown_routed)}")

    if set(patrol_route) != routed_cabins:
        only_route = sorted(set(patrol_route) - routed_cabins)
        only_routes = sorted(routed_cabins - set(patrol_route))
        raise ValueError(
            "patrol route and partition routing cover different cabins"
            + (f"; missing a partition route: {','.join(only_route)}" if only_route else "")
            + (f"; absent from patrol route: {','.join(only_routes)}" if only_routes else "")
        )

    # A reorg must restate the whole partition picture; silently dropping an
    # active cabin is exactly the "downstream 口径 mismatch" the atomic reorg
    # exists to prevent.
    missing = sorted(active - routed_cabins)
    if missing:
        raise ValueError(f"manifest drops active cabins from the routing: {','.join(missing)}")

    if known_partitions is not None:
        for partition_id in sorted(sensor_partitions.values()):
            if partition_id not in known_partitions and partition_id not in partition_routes:
                raise ValueError(f"sensor binds to unknown partition: {partition_id}")
    else:
        for partition_id in sorted(sensor_partitions.values()):
            if partition_id not in partition_routes:
                raise ValueError(f"sensor binds to unknown partition: {partition_id}")
    return normalised
