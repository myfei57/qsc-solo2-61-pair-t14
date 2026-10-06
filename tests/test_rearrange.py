"""Behavioural tests for the atomic rearrange coordinator."""

from __future__ import annotations

import tempfile
import threading
import unittest

from tunnelnet.audit import AuditRegistry
from tunnelnet.cabin import Cabin, PartitionTable
from tunnelnet.cabin import Registry as CabinRegistry
from tunnelnet.cable import MovingAverage
from tunnelnet.console.server import Server
from tunnelnet.env import Environment
from tunnelnet.patrol import PatrolService
from tunnelnet.rearrange import (
    RearrangeConflict,
    RearrangeCoordinator,
    RearrangeError,
    RearrangePlan,
    planned_route,
)
from tunnelnet.store import Store


def make_stack(tmp: str, fault=None):
    """A coordinator wired over real components with a seeded baseline."""

    store = Store.open(tmp)
    cabins = CabinRegistry()
    cabins.upsert(Cabin(id="cabin-1", zone="zone-1", partition="part-1"))
    cabins.upsert(Cabin(id="cabin-2", zone="zone-2", partition="part-2"))
    cabins.upsert(Cabin(id="cabin-3", zone="zone-3", partition="part-3"))
    partitions = PartitionTable()
    partitions.repartition("part-1", "cabin-1")
    partitions.repartition("part-2", "cabin-2")
    partitions.repartition("part-3", "cabin-3")
    env = Environment(MovingAverage(1), partitions, 75.0)
    env.bind_sensor_at("sensor-1", "part-1")
    env.bind_sensor_at("sensor-2", "part-2")
    patrol = PatrolService(cabins)
    audit = AuditRegistry()
    coordinator = RearrangeCoordinator(
        cabins, partitions, env, patrol, store, audit, fault_injector=fault
    )
    env.set_attribution_provider(coordinator.stamp_for)
    coordinator.adopt_current("seed", "t0")
    return coordinator, cabins, partitions, env, patrol, audit, store


class AtomicCommitCase(unittest.TestCase):
    def test_commit_applies_all_three_components_together(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, partitions, env, patrol, audit, store = make_stack(tmp)
            receipt = coordinator.submit(
                RearrangePlan(
                    request_id="r1",
                    assignments={"part-1": "cabin-2"},
                    bindings={"sensor-1": "part-2"},
                    at="t1",
                )
            )
            self.assertEqual(receipt.status, "committed")
            self.assertEqual(receipt.generation, 2)
            self.assertEqual(partitions.cabin_of("part-1"), "cabin-2")
            self.assertEqual(env.partition_of("sensor-1"), "part-2")
            self.assertEqual(env.route_alarm("sensor-1"), "cabin-2")
            self.assertEqual(patrol.route_ids(), ["cabin-2", "cabin-3", "cabin-1"])
            self.assertFalse(coordinator.drifted())
            self.assertEqual(coordinator.current_generation(), 2)
            events = [record.event for record in audit.records()]
            self.assertIn("rearrange_committed", events)
            journal = store.load("rearrange", "r1")
            self.assertEqual(journal["status"], "committed")
            self.assertEqual(journal["snapshot"]["generation"], 2)
            self.assertEqual(journal["snapshot"]["mapping"]["part-1"], "cabin-2")

    def test_revision_bumps_once_per_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, partitions, _, _, _, _ = make_stack(tmp)
            before = partitions.revision()
            coordinator.submit(
                RearrangePlan(
                    request_id="r1",
                    assignments={"part-1": "cabin-2", "part-2": "cabin-3"},
                )
            )
            self.assertEqual(partitions.revision(), before + 1)

    def test_view_is_a_consistent_read_of_the_live_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, _, _, _, _, _ = make_stack(tmp)
            coordinator.submit(
                RearrangePlan(request_id="r1", assignments={"part-1": "cabin-2"})
            )
            view = coordinator.current_view()
            self.assertEqual(view.generation, 2)
            self.assertEqual(view.mapping["part-1"], "cabin-2")
            self.assertEqual(view.bindings["sensor-1"], "part-1")
            self.assertEqual(view.route, ("cabin-2", "cabin-3", "cabin-1"))
            latest = coordinator.generations()[-1]
            self.assertTrue(latest.matches(view))
            self.assertIn("generation=2", coordinator.describe())
            self.assertEqual(coordinator.state().confirmed, 2)


class RollbackCase(unittest.TestCase):
    def test_failure_at_routing_restores_partitions(self) -> None:
        def fault(stage: str) -> None:
            if stage == "routing":
                raise RuntimeError("routing failed")

        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, partitions, env, patrol, audit, store = make_stack(tmp, fault=fault)
            route_before = patrol.route_ids()
            revision_before = partitions.revision()
            receipt = coordinator.submit(
                RearrangePlan(
                    request_id="r2",
                    assignments={"part-1": "cabin-3"},
                    bindings={"sensor-1": "part-3"},
                )
            )
            self.assertEqual(receipt.status, "rolled_back")
            self.assertEqual(receipt.generation, 1)
            self.assertIn("routing failed", receipt.detail)
            self.assertEqual(partitions.cabin_of("part-1"), "cabin-1")
            self.assertEqual(env.partition_of("sensor-1"), "part-1")
            self.assertEqual(patrol.route_ids(), route_before)
            self.assertEqual(partitions.revision(), revision_before + 2)
            self.assertFalse(coordinator.drifted())
            self.assertEqual(coordinator.current_generation(), 1)
            events = [record.event for record in audit.records()]
            self.assertIn("rearrange_rolled_back", events)
            self.assertEqual(store.load("rearrange", "r2")["status"], "rolled_back")

    def test_failure_at_patrol_restores_everything(self) -> None:
        def fault(stage: str) -> None:
            if stage == "patrol":
                raise RuntimeError("route install failed")

        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, partitions, env, patrol, _, _ = make_stack(tmp, fault=fault)
            receipt = coordinator.submit(
                RearrangePlan(
                    request_id="r3",
                    assignments={"part-1": "cabin-2"},
                    bindings={"sensor-1": "part-2"},
                )
            )
            self.assertEqual(receipt.status, "rolled_back")
            self.assertEqual(partitions.mapping(), {"part-1": "cabin-1", "part-2": "cabin-2", "part-3": "cabin-3"})
            self.assertEqual(env.bindings(), {"sensor-1": "part-1", "sensor-2": "part-2"})
            self.assertEqual(patrol.route_ids(), ["cabin-1", "cabin-2", "cabin-3"])
            self.assertFalse(coordinator.drifted())

    def test_failed_request_is_not_retried_under_the_same_key(self) -> None:
        calls: list[str] = []

        def fault(stage: str) -> None:
            calls.append(stage)
            if stage == "routing":
                raise RuntimeError("boom")

        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, _, _, _, _, _ = make_stack(tmp, fault=fault)
            plan = RearrangePlan(request_id="r4", assignments={"part-1": "cabin-2"})
            first = coordinator.submit(plan)
            self.assertEqual(first.status, "rolled_back")
            replay = coordinator.submit(plan)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.status, "rolled_back")
            self.assertEqual(calls, ["partitions", "routing"])


class IdempotencyCase(unittest.TestCase):
    def test_same_request_id_replays_without_reapplying(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, partitions, _, _, _, store = make_stack(tmp)
            plan = RearrangePlan(request_id="r1", assignments={"part-1": "cabin-2"})
            first = coordinator.submit(plan)
            self.assertEqual(first.status, "committed")
            revision = partitions.revision()
            replay = coordinator.submit(plan)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.generation, first.generation)
            self.assertEqual(partitions.revision(), revision)
            self.assertEqual(coordinator.current_generation(), 2)
            self.assertEqual(store.count("rearrange"), 1)

    def test_ledger_survives_a_restart_via_the_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, _, _, _, _, store = make_stack(tmp)
            coordinator.submit(RearrangePlan(request_id="r1", assignments={"part-1": "cabin-2"}))

            cabins = CabinRegistry()
            cabins.upsert(Cabin(id="cabin-1"))
            cabins.upsert(Cabin(id="cabin-2"))
            partitions = PartitionTable()
            env = Environment(MovingAverage(1), partitions, 75.0)
            patrol = PatrolService(cabins)
            restarted = RearrangeCoordinator(
                cabins, partitions, env, patrol, store, AuditRegistry()
            )
            replay = restarted.submit(
                RearrangePlan(request_id="r1", assignments={"part-1": "cabin-2"})
            )
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.status, "committed")
            self.assertEqual(replay.generation, 2)
            self.assertEqual(partitions.cabin_of("part-1"), "")

    def test_concurrent_same_key_commits_exactly_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, _, _, _, _, _ = make_stack(tmp)
            plan = RearrangePlan(request_id="race", assignments={"part-1": "cabin-2"})
            receipts = []
            threads = [
                threading.Thread(target=lambda: receipts.append(coordinator.submit(plan)))
                for _ in range(8)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            fresh = [receipt for receipt in receipts if not receipt.replayed]
            self.assertEqual(len(fresh), 1)
            self.assertEqual(coordinator.current_generation(), 2)
            self.assertEqual(
                {receipt.request_id for receipt in coordinator.ledger()}, {"seed", "race"}
            )

    def test_concurrent_distinct_plans_serialize_with_contiguous_generations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, _, _, _, _, _ = make_stack(tmp)

            def work(index: int) -> None:
                coordinator.submit(
                    RearrangePlan(
                        request_id=f"bulk-{index}",
                        assignments={"part-1": f"cabin-{(index % 3) + 1}"},
                    )
                )

            threads = [threading.Thread(target=work, args=(index,)) for index in range(12)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            generations = [snapshot.generation for snapshot in coordinator.generations()]
            self.assertEqual(generations, list(range(1, 14)))
            self.assertFalse(coordinator.drifted())
            self.assertEqual(len(coordinator.ledger()), 13)


class ConcurrencyGuardCase(unittest.TestCase):
    def test_base_generation_conflict_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, _, _, _, _, _ = make_stack(tmp)
            with self.assertRaises(RearrangeConflict):
                coordinator.submit(
                    RearrangePlan(
                        request_id="r1",
                        assignments={"part-1": "cabin-2"},
                        base_generation=9,
                    )
                )
            receipt = coordinator.submit(
                RearrangePlan(
                    request_id="r2",
                    assignments={"part-1": "cabin-2"},
                    base_generation=1,
                )
            )
            self.assertEqual(receipt.status, "committed")

    def test_validation_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, _, _, _, _, _ = make_stack(tmp)
            with self.assertRaises(RearrangeError):
                coordinator.submit(RearrangePlan(request_id="", assignments={"part-1": "cabin-1"}))
            with self.assertRaises(RearrangeError):
                coordinator.submit(RearrangePlan(request_id="r1"))
            with self.assertRaises(RearrangeError):
                coordinator.submit(
                    RearrangePlan(request_id="r2", assignments={"part-1": "ghost"})
                )
            with self.assertRaises(RearrangeError):
                coordinator.submit(
                    RearrangePlan(request_id="r3", bindings={"sensor-1": ""})
                )
            self.assertEqual(coordinator.current_generation(), 1)


class RevertCase(unittest.TestCase):
    def test_revert_restores_the_last_confirmed_snapshot_as_a_new_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, partitions, env, patrol, audit, _ = make_stack(tmp)
            coordinator.submit(RearrangePlan(request_id="r1", assignments={"part-1": "cabin-2"}))
            coordinator.submit(RearrangePlan(request_id="r2", assignments={"part-2": "cabin-1"}))
            self.assertEqual(coordinator.current_generation(), 3)

            receipt = coordinator.revert("rv1")
            self.assertEqual(receipt.status, "reverted")
            self.assertEqual(receipt.generation, 4)
            target = coordinator.generations()[1]
            self.assertEqual(partitions.mapping(), target.mapping)
            self.assertEqual(env.bindings(), target.bindings)
            self.assertEqual(tuple(patrol.route_ids()), target.route)
            self.assertFalse(coordinator.drifted())
            events = [record.event for record in audit.records()]
            self.assertIn("rearrange_reverted", events)

            again = coordinator.revert("rv1")
            self.assertTrue(again.replayed)
            self.assertEqual(coordinator.current_generation(), 4)

    def test_revert_without_an_earlier_snapshot_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, _, _, _, _, _ = make_stack(tmp)
            with self.assertRaises(RearrangeError):
                coordinator.revert("rv1")


class AlarmReviewCase(unittest.TestCase):
    def test_alarms_keep_their_generation_and_review_against_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, _, env, _, _, _ = make_stack(tmp)
            self.assertTrue(env.sample("sensor-1", 90.0, "t1"))
            coordinator.submit(
                RearrangePlan(request_id="r1", assignments={"part-1": "cabin-3"})
            )
            self.assertTrue(env.sample("sensor-1", 95.0, "t2"))

            first, second = env.alarms()
            self.assertEqual((first.generation, first.partition, first.cabin), (1, "part-1", "cabin-1"))
            self.assertEqual((second.generation, second.partition, second.cabin), (2, "part-1", "cabin-3"))

            reviews = coordinator.review_alarms(env.alarms())
            self.assertEqual(len(reviews), 2)
            self.assertEqual(reviews[0]["stamped"], {"generation": 1, "partition": "part-1", "cabin": "cabin-1"})
            self.assertEqual(reviews[0]["verified"], {"partition": "part-1", "cabin": "cabin-1", "consistent": True})
            self.assertEqual(reviews[0]["current"]["cabin"], "cabin-3")
            self.assertEqual(reviews[1]["stamped"]["cabin"], "cabin-3")
            self.assertTrue(reviews[1]["verified"]["consistent"])
            self.assertEqual(coordinator.inconsistent_alarm_count(), 0)
            self.assertEqual(env.alarms()[0].cabin, "cabin-1")

    def test_stamping_never_sees_a_half_applied_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, _, env, _, _, _ = make_stack(tmp)
            stamps: list[tuple[int, str, str]] = []
            stop = threading.Event()

            def raise_alarms() -> None:
                while not stop.is_set():
                    stamps.append(coordinator.stamp_for("sensor-1"))

            def rearrange(index: int) -> None:
                coordinator.submit(
                    RearrangePlan(
                        request_id=f"r{index}",
                        assignments={"part-1": f"cabin-{(index % 3) + 1}"},
                    )
                )

            sampler = threading.Thread(target=raise_alarms)
            sampler.start()
            workers = [threading.Thread(target=rearrange, args=(index,)) for index in range(6)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
            stop.set()
            sampler.join()

            self.assertTrue(stamps)
            snapshots = {snapshot.generation: snapshot for snapshot in coordinator.generations()}
            for generation, partition, cabin in stamps:
                snapshot = snapshots[generation]
                self.assertEqual(snapshot.bindings.get("sensor-1", ""), partition)
                self.assertEqual(snapshot.mapping.get(partition, ""), cabin)


class DriftCase(unittest.TestCase):
    def test_bypassing_the_coordinator_shows_up_as_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            coordinator, _, partitions, _, _, _, _ = make_stack(tmp)
            self.assertFalse(coordinator.drifted())
            partitions.repartition("part-1", "cabin-9")
            self.assertTrue(coordinator.drifted())
            receipt = coordinator.adopt_current("repair")
            self.assertEqual(receipt.status, "adopted")
            self.assertFalse(coordinator.drifted())
            again = coordinator.adopt_current("repair-2")
            self.assertEqual(again.status, "unchanged")

    def test_planned_route_orders_mapped_cabins_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, cabins, _, _, _, _, _ = make_stack(tmp)
            active = cabins.active()
            route = planned_route({"part-2": "cabin-2", "part-1": "cabin-1"}, active)
            self.assertEqual([point.cabin_id for point in route], ["cabin-1", "cabin-2", "cabin-3"])
            self.assertEqual([point.order for point in route], [0, 1, 2])
            swapped = planned_route({"z": "cabin-3", "a": "cabin-1"}, active)
            self.assertEqual([point.cabin_id for point in swapped], ["cabin-1", "cabin-3", "cabin-2"])
            duplicate = planned_route({"a": "cabin-1", "b": "cabin-1"}, active)
            self.assertEqual([point.cabin_id for point in duplicate], ["cabin-1", "cabin-2", "cabin-3"])


class RearrangeConsoleCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.server = Server(Store.open(f"{self._tmp.name}/data"), web_dir="web")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def call(self, method: str, path: str, payload: dict | None = None, query: dict | None = None):
        import json

        response = self.server.dispatch(method, path, payload, query)
        body = response.body.decode("utf-8")
        parsed = json.loads(body) if response.content_type.startswith("application/json") else body
        return response.status, parsed

    def test_submit_review_and_revert_over_http(self) -> None:
        status, state = self.call("GET", "/api/rearrange")
        self.assertEqual(status, 200)
        self.assertEqual(state["state"]["generation"], 1)
        self.assertFalse(state["state"]["drifted"])
        self.assertEqual(state["view"]["mapping"]["part-1"], "cabin-1")

        status, alarmed = self.call(
            "POST", "/api/env/sample", {"sensor_id": "sensor-1", "value": 200.0, "at": "t1"}
        )
        self.assertTrue(alarmed["alarmed"])

        plan = {
            "request_id": "op-1",
            "assignments": {"part-1": "cabin-2"},
            "bindings": {"sensor-1": "part-2"},
        }
        status, receipt = self.call("POST", "/api/rearrange", plan)
        self.assertEqual(status, 200)
        self.assertEqual(receipt["status"], "committed")
        self.assertEqual(receipt["generation"], 2)
        self.assertFalse(receipt["replayed"])

        _, overview = self.call("GET", "/api/env")
        self.assertEqual(overview["sensor_1_cabin"], "cabin-2")
        _, patrols = self.call("GET", "/api/patrols")
        self.assertEqual(patrols["route_ids"], ["cabin-2", "cabin-3", "cabin-1"])

        status, replay = self.call("POST", "/api/rearrange", plan)
        self.assertEqual(status, 200)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["generation"], 2)

        status, history = self.call("GET", "/api/rearrange/history")
        self.assertEqual(status, 200)
        self.assertEqual(len(history["generations"]), 2)
        self.assertEqual(history["journal_records"], 1)
        self.assertEqual(history["generations"][0]["request_id"], "seed")

        status, alarms = self.call("GET", "/api/rearrange/alarms")
        self.assertEqual(status, 200)
        self.assertEqual(alarms["count"], 1)
        item = alarms["items"][0]
        self.assertEqual(item["stamped"], {"generation": 1, "partition": "part-1", "cabin": "cabin-1"})
        self.assertTrue(item["verified"]["consistent"])
        self.assertEqual(item["current"]["cabin"], "cabin-2")

        status, reverted = self.call("POST", "/api/rearrange/revert", {"request_id": "rv-1"})
        self.assertEqual(status, 200)
        self.assertEqual(reverted["status"], "reverted")
        self.assertEqual(reverted["generation"], 3)
        _, overview = self.call("GET", "/api/env")
        self.assertEqual(overview["sensor_1_cabin"], "cabin-1")
        _, alarms = self.call("GET", "/api/rearrange/alarms")
        self.assertEqual(alarms["items"][0]["stamped"]["cabin"], "cabin-1")

        _, audit = self.call("GET", "/api/audit/query", query={"event": ["rearrange_committed"]})
        self.assertEqual(len(audit["filtered"]), 1)
        _, audit = self.call("GET", "/api/audit/query", query={"event": ["rearrange_reverted"]})
        self.assertEqual(len(audit["filtered"]), 1)

    def test_conflict_and_validation_over_http(self) -> None:
        status, body = self.call(
            "POST",
            "/api/rearrange",
            {"request_id": "op-1", "assignments": {"part-1": "cabin-2"}, "base_generation": 9},
        )
        self.assertEqual(status, 409)
        self.assertIn("base generation", body["error"])

        status, body = self.call("POST", "/api/rearrange", {"request_id": "op-2"})
        self.assertEqual(status, 400)
        self.assertIn("changes nothing", body["error"])

        status, body = self.call(
            "POST", "/api/rearrange", {"request_id": "op-3", "assignments": {"part-1": "ghost"}}
        )
        self.assertEqual(status, 400)
        self.assertIn("not found", body["error"])

        status, body = self.call("POST", "/api/rearrange", {"assignments": {"part-1": "cabin-1"}})
        self.assertEqual(status, 400)
        self.assertIn("request_id", body["error"])

        status, body = self.call(
            "POST", "/api/rearrange", {"request_id": "op-4", "assignments": ["part-1"]}
        )
        self.assertEqual(status, 400)
        self.assertIn("must be an object", body["error"])

        status, body = self.call("POST", "/api/rearrange/revert", {"request_id": "rv-1"})
        self.assertEqual(status, 400)
        self.assertIn("no earlier confirmed snapshot", body["error"])

    def test_legacy_partition_endpoint_is_atomic_and_audited(self) -> None:
        status, body = self.call(
            "POST", "/api/env/partition", {"partition_id": "part-3", "cabin_id": "cabin-1"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["revision"], 4)
        _, state = self.call("GET", "/api/rearrange")
        self.assertEqual(state["state"]["generation"], 2)
        self.assertFalse(state["state"]["drifted"])
        _, history = self.call("GET", "/api/rearrange/history")
        self.assertEqual(history["journal_records"], 1)

    def test_health_and_describe_include_rearrange(self) -> None:
        status, checks = self.call("GET", "/api/health/checks")
        row = {check["name"]: check for check in checks["checks"]}["rearrange"]
        self.assertEqual(row["status"], "ok")
        self.assertIn("generation=1", row["detail"])

        _, describe = self.call("GET", "/api/describe")
        self.assertIn("rearrange", describe)

        _, snapshot = self.call("GET", "/api/snapshot")
        self.assertEqual(snapshot["rearrange"]["generation"], 1)


if __name__ == "__main__":
    unittest.main()
