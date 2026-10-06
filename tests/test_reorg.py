"""Domain tests for atomic, versioned zone reorganisations."""

from __future__ import annotations

import tempfile
import threading
import unittest

from tunnelnet.audit import AuditRegistry
from tunnelnet.cabin import Cabin, PartitionTable, Registry as CabinRegistry
from tunnelnet.cable import MovingAverage
from tunnelnet.env import Environment
from tunnelnet.patrol import PatrolService
from tunnelnet.reorg import (
    EFFECT_IMMEDIATE,
    ReorgConflict,
    ReorgCoordinator,
    STATUS_ACTIVE,
    STATUS_ARMED,
    STATUS_CANCELLED,
    STATUS_CONFIRMED,
    STATUS_FAILED,
    STATUS_ROLLED_BACK,
    fingerprint,
    validate_manifest,
)
from tunnelnet.reorg.model import ZoneLayout
from tunnelnet.store import Store


def build_wiring(tmp: str) -> tuple[ReorgCoordinator, CabinRegistry, PartitionTable, Environment, PatrolService]:
    store = Store.open(tmp)
    cabins = CabinRegistry()
    for index in range(1, 4):
        cabins.upsert(Cabin(id=f"cabin-{index}", zone=f"zone-{index}", partition=f"part-{index}"))
    partitions = PartitionTable()
    for index in range(1, 4):
        partitions.repartition(f"part-{index}", f"cabin-{index}")
    env = Environment(MovingAverage(3), partitions, 75.0)
    for index in range(1, 4):
        env.bind_sensor_at(f"sensor-{index}", f"part-{index}")
    patrol = PatrolService(cabins)
    audit = AuditRegistry()
    coordinator = ReorgCoordinator(store, cabins, partitions, env, patrol, audit)
    coordinator.bootstrap(at="2026-01-01T00:00:00Z")
    return coordinator, cabins, partitions, env, patrol


SWAP = {
    "partitions": {"part-1": "cabin-2", "part-2": "cabin-1", "part-3": "cabin-3"},
    "route": ["cabin-3", "cabin-1", "cabin-2"],
    "sensors": {"sensor-1": "part-1", "sensor-2": "part-2", "sensor-3": "part-3"},
}
ROTATE = {
    "partitions": {"part-1": "cabin-3", "part-2": "cabin-2", "part-3": "cabin-1"},
    "route": ["cabin-2", "cabin-3", "cabin-1"],
    "sensors": {"sensor-1": "part-1", "sensor-2": "part-2", "sensor-3": "part-3"},
}


class ManifestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.coord, self.cabins, *_ = build_wiring(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_accepts_full_consistent_manifest(self) -> None:
        normalised = validate_manifest(SWAP, self.cabins)
        self.assertEqual(normalised["route"], SWAP["route"])

    def test_rejects_dropped_cabin(self) -> None:
        bad = {"partitions": {"part-1": "cabin-1"}, "route": ["cabin-1"], "sensors": {}}
        with self.assertRaises(ValueError):
            validate_manifest(bad, self.cabins)

    def test_rejects_route_routing_mismatch(self) -> None:
        # part-3 routes to cabin-3 but the patrol route visits cabin-1 twice
        # and never cabin-3: the two projections cover different cabin sets.
        bad = {
            "partitions": {"part-1": "cabin-1", "part-2": "cabin-2", "part-3": "cabin-3"},
            "route": ["cabin-1", "cabin-2", "cabin-1"],
            "sensors": {},
        }
        with self.assertRaises(ValueError):
            validate_manifest(bad, self.cabins)

    def test_rejects_duplicate_route_stop(self) -> None:
        bad = {
            "partitions": {"part-1": "cabin-1", "part-2": "cabin-2", "part-3": "cabin-3"},
            "route": ["cabin-1", "cabin-1", "cabin-2"],
            "sensors": {},
        }
        with self.assertRaises(ValueError):
            validate_manifest(bad, self.cabins)

    def test_rejects_unknown_sensor_partition(self) -> None:
        bad = dict(SWAP)
        bad["sensors"] = {"sensor-9": "part-ghost"}
        with self.assertRaises(ValueError):
            validate_manifest(bad, self.cabins)


class ReorgLifecycleCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.coord, self.cabins, self.partitions, self.env, self.patrol = build_wiring(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_bootstrap_is_v1_confirmed(self) -> None:
        self.assertEqual(self.coord.current_version(), 1)
        layout = self.coord.current_layout()
        self.assertTrue(layout.confirmed)
        self.assertEqual(layout.origin, "bootstrap")
        self.assertEqual(self.env.layout_version(), 1)
        # bootstrapping again is idempotent and does not mint a new version
        self.coord.bootstrap()
        self.assertEqual(self.coord.current_version(), 1)

    def test_immediate_reorg_activates_three_projections_at_once(self) -> None:
        request, replay = self.coord.submit(
            "r1", SWAP, effective=EFFECT_IMMEDIATE, at="2026-02-01T00:00:00Z"
        )
        self.assertFalse(replay)
        self.assertEqual(request.status, STATUS_ACTIVE)
        self.assertEqual(self.coord.current_version(), 2)
        # partition routing
        self.assertEqual(self.partitions.cabin_of("part-1"), "cabin-2")
        # environment router
        self.assertEqual(self.env.route_alarm("sensor-1"), "cabin-2")
        # patrol route, same commit
        self.assertEqual(self.patrol.route_ids(), ["cabin-3", "cabin-1", "cabin-2"])

    def test_round_boundary_reorg_waits_for_round_end(self) -> None:
        request, _ = self.coord.submit("r1", SWAP, at="2026-02-01T00:00:00Z")
        self.assertEqual(request.status, STATUS_ARMED)
        # Nothing downstream moves while the round is in progress.
        self.assertEqual(self.coord.current_version(), 1)
        self.assertEqual(self.partitions.cabin_of("part-1"), "cabin-1")
        self.assertEqual(self.patrol.route_ids(), ["cabin-1", "cabin-2", "cabin-3"])

        self.patrol.check_in("cabin-1", "2026-02-02T00:00:00Z")
        activated = self.coord.close_round("2026-02-03T00:00:00Z")
        self.assertEqual(activated.status, STATUS_ACTIVE)
        self.assertEqual(self.coord.current_version(), 2)
        self.assertEqual(self.partitions.cabin_of("part-1"), "cabin-2")
        self.assertEqual(self.patrol.route_ids(), ["cabin-3", "cabin-1", "cabin-2"])
        # The archived round walked the OLD route, so an operator never inspects
        # one round against two layouts.
        archived = self.patrol.rounds()[-1]
        self.assertEqual(archived.route, ["cabin-1", "cabin-2", "cabin-3"])
        self.assertEqual([c.cabin_id for c in archived.check_ins], ["cabin-1"])
        # A fresh round opened on the new route with an empty check-in ledger.
        self.assertEqual(self.patrol.checked_count(), 0)

    def test_confirm_then_rollback_restores_last_confirmed(self) -> None:
        self.coord.submit("r1", SWAP, effective=EFFECT_IMMEDIATE)
        confirmed = self.coord.confirm("r1")
        self.assertEqual(confirmed.status, STATUS_CONFIRMED)
        self.assertEqual(self.coord.last_confirmed().version, 2)

        self.coord.submit("r2", ROTATE, effective=EFFECT_IMMEDIATE)
        self.assertEqual(self.coord.current_version(), 3)
        rolled = self.coord.rollback("r2")
        self.assertEqual(rolled.status, STATUS_ROLLED_BACK)
        self.assertEqual(rolled.restored_version, 2)
        # Content restored to v2, but as a new append-only compensating version.
        self.assertGreater(self.coord.current_version(), 3)
        layout = self.coord.current_layout()
        self.assertEqual(layout.origin, "rollback")
        self.assertEqual(layout.partition_routes, SWAP["partitions"])
        self.assertEqual(layout.patrol_route, SWAP["route"])
        self.assertEqual(self.partitions.cabin_of("part-1"), "cabin-2")

    def test_cannot_rollback_an_already_confirmed_reorg(self) -> None:
        self.coord.submit("r1", SWAP, effective=EFFECT_IMMEDIATE)
        self.coord.confirm("r1")
        with self.assertRaises(ReorgConflict):
            self.coord.rollback("r1")

    def test_cancel_armed_reorg(self) -> None:
        self.coord.submit("r1", SWAP)
        cancelled = self.coord.rollback("r1")
        self.assertEqual(cancelled.status, STATUS_CANCELLED)
        self.assertEqual(self.coord.current_version(), 1)
        self.assertEqual(self.partitions.cabin_of("part-1"), "cabin-1")

    def test_idempotent_replay_returns_same_request(self) -> None:
        first, replay1 = self.coord.submit("dup", SWAP)
        self.assertFalse(replay1)
        second, replay2 = self.coord.submit("dup", SWAP)
        self.assertTrue(replay2)
        self.assertEqual(first.request_id, second.request_id)
        self.assertEqual(second.candidate_version, first.candidate_version)

    def test_concurrent_pending_conflicts(self) -> None:
        self.coord.submit("r1", SWAP)
        with self.assertRaises(ReorgConflict):
            self.coord.submit("r2", SWAP)

    def test_stale_base_version_conflicts(self) -> None:
        with self.assertRaises(ReorgConflict):
            self.coord.submit("r1", SWAP, base_version=99)

    def test_identical_manifest_is_rejected(self) -> None:
        current = self.coord.current_layout()
        same = {
            "partitions": dict(current.partition_routes),
            "route": list(current.patrol_route),
            "sensors": dict(current.sensor_partitions),
        }
        with self.assertRaises(ValueError):
            self.coord.submit("noop", same)


class AlarmPinningCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.coord, self.cabins, self.partitions, self.env, self.patrol = build_wiring(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _raise(self, sensor: str, at: str) -> dict[str, object]:
        self.assertTrue(self.env.sample(sensor, 200.0, at))
        return self.env.alarms()[-1].as_dict()

    def test_alarm_birth_attribution_is_frozen_across_reorg(self) -> None:
        before = self._raise("sensor-2", "2026-02-01T00:00:00Z")
        self.assertEqual(before["birth_version"], 1)
        self.assertEqual(before["birth_cabin"], "cabin-2")
        self.assertEqual(before["birth_partition"], "part-2")

        # Staged reorg must not reroute the alarm being handled.
        self.coord.submit("r1", SWAP, at="2026-02-02T00:00:00Z")
        during = self._raise("sensor-2", "2026-02-03T00:00:00Z")
        self.assertEqual(during["birth_version"], 1)
        self.assertEqual(during["birth_cabin"], "cabin-2")

        self.coord.close_round("2026-02-04T00:00:00Z")
        after = self._raise("sensor-2", "2026-02-05T00:00:00Z")
        self.assertEqual(after["birth_version"], 2)
        self.assertEqual(after["birth_cabin"], "cabin-1")

        # Historical alarm is still attributable under its birth layout.
        review = self.coord.review_alarm(before)
        self.assertTrue(review["birth_attribution_intact"])
        self.assertEqual(review["birth_layout_cabin"], "cabin-2")
        self.assertTrue(review["routing_changed_since_birth"])
        self.assertEqual(review["current_cabin"], "cabin-1")

    def test_as_of_resolution(self) -> None:
        self.coord.submit("r1", SWAP, effective=EFFECT_IMMEDIATE, at="2026-02-04T00:00:00Z")
        self.assertIsNone(self.coord.layout_effective_at("2025-12-31T00:00:00Z"))
        self.assertEqual(self.coord.layout_effective_at("2026-02-01T00:00:00Z").version, 1)
        self.assertEqual(self.coord.layout_effective_at("2026-02-05T00:00:00Z").version, 2)


class AtomicFailureCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.coord, self.cabins, self.partitions, self.env, self.patrol = build_wiring(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_projection_failure_rolls_back_everything(self) -> None:
        self.coord.submit("good", SWAP, effective=EFFECT_IMMEDIATE)
        self.coord.confirm("good")

        original = self.env.apply_layout
        def sabotage(**kwargs):  # noqa: ANN003
            raise RuntimeError("env down")

        self.env.apply_layout = sabotage
        with self.assertRaises(RuntimeError):
            self.coord.submit("bad", ROTATE, effective=EFFECT_IMMEDIATE)
        self.env.apply_layout = original

        failed = self.coord.request("bad")
        self.assertEqual(failed.status, STATUS_FAILED)
        # Live version and every projection remain on the pre-commit layout.
        self.assertEqual(self.coord.current_layout().partition_routes, SWAP["partitions"])
        self.assertEqual(self.partitions.mapping(), SWAP["partitions"])
        self.assertEqual(self.patrol.route_ids(), SWAP["route"])
        self.assertEqual(self.env.layout_version(), self.coord.current_version())
        # The failed candidate version slot is retained append-only, never live.
        self.assertIsNotNone(self.coord.layout(failed.candidate_version))
        self.assertNotIn(self.coord.current_version(), [failed.candidate_version])


class HotfixCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.coord, self.cabins, self.partitions, self.env, self.patrol = build_wiring(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_legacy_partition_edit_is_versioned_and_immediate(self) -> None:
        layout = self.coord.record_hotfix("part-3", "cabin-1", at="2026-02-01T00:00:00Z")
        self.assertEqual(layout.origin, "hotfix")
        self.assertTrue(layout.confirmed)
        self.assertEqual(self.coord.current_version(), layout.version)
        self.assertEqual(self.env.layout_version(), layout.version)
        self.assertEqual(self.partitions.cabin_of("part-3"), "cabin-1")

    def test_noop_hotfix_does_not_mint_version(self) -> None:
        before = self.coord.current_version()
        self.coord.record_hotfix("part-1", "cabin-1")
        self.assertEqual(self.coord.current_version(), before)


class ConcurrencyCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.coord, *_ = build_wiring(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_parallel_submits_serialise_to_one_winner(self) -> None:
        outcomes: list[object] = []

        def worker(index: int) -> None:
            try:
                request, _ = self.coord.submit(f"c{index}", SWAP)
                outcomes.append(("ok", request.status))
            except ReorgConflict as exc:
                outcomes.append(("conflict", str(exc)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        winners = [item for item in outcomes if item[0] == "ok"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(outcomes), 8)


class FingerprintCase(unittest.TestCase):
    def test_fingerprint_stable_order_independent(self) -> None:
        a = fingerprint({"p1": "c1", "p2": "c2"}, ["c1", "c2"], {"s1": "p1"})
        b = fingerprint({"p2": "c2", "p1": "c1"}, ["c1", "c2"], {"s1": "p1"})
        self.assertEqual(a, b)

    def test_fingerprint_changes_with_route_order(self) -> None:
        a = fingerprint({"p1": "c1", "p2": "c2"}, ["c1", "c2"], {})
        b = fingerprint({"p1": "c1", "p2": "c2"}, ["c2", "c1"], {})
        self.assertNotEqual(a, b)

    def test_layout_round_trip(self) -> None:
        layout = ZoneLayout(version=3, partition_routes={"p1": "c1"}, patrol_route=["c1"], restores_version=1)
        restored = ZoneLayout.from_dict(layout.as_dict())
        self.assertEqual(restored.restores_version, 1)
        self.assertEqual(restored.anchor_version(), 1)
        self.assertEqual(restored.partition_routes, {"p1": "c1"})


if __name__ == "__main__":
    unittest.main()
