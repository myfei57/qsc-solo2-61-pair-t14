"""End to end HTTP tests for the atomic zone reorganisation endpoints."""

from __future__ import annotations

import json
import tempfile
import unittest

from tunnelnet.console.server import Server
from tunnelnet.store import Store


def swap_manifest() -> dict[str, object]:
    return {
        "partitions": {"part-1": "cabin-2", "part-2": "cabin-1", "part-3": "cabin-3"},
        "route": ["cabin-3", "cabin-1", "cabin-2"],
        "sensors": {"sensor-1": "part-1", "sensor-2": "part-2", "sensor-3": "part-3"},
    }


class ReorgConsoleCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.server = Server(Store.open(f"{self._tmp.name}/data"), web_dir="web")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def call(self, method: str, path: str, payload: dict | None = None, query: dict | None = None):
        response = self.server.dispatch(method, path, payload, query)
        body = json.loads(response.body.decode("utf-8"))
        return response.status, body

    def test_initial_layout_is_bootstrapped_v1(self) -> None:
        status, state = self.call("GET", "/api/reorg")
        self.assertEqual(status, 200)
        self.assertEqual(state["current_version"], 1)
        self.assertEqual(state["last_confirmed"], 1)
        self.assertEqual(state["pending"], None)
        status, snapshot = self.call("GET", "/api/reorg/snapshot")
        self.assertEqual(snapshot["route"], ["cabin-1", "cabin-2", "cabin-3"])

    def test_round_boundary_flow_over_http(self) -> None:
        # An alarm before the reorg is born on v1 -> sensor-2 routes to cabin-2.
        status, _ = self.call(
            "POST", "/api/env/sample", {"sensor_id": "sensor-2", "value": 200.0, "at": "2026-03-01T00:00:00Z"}
        )
        self.assertEqual(status, 200)

        status, body = self.call(
            "POST",
            "/api/reorg/submit",
            {"request_id": "r1", "manifest": swap_manifest(), "at": "2026-03-02T00:00:00Z"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["request"]["status"], "armed")
        self.assertEqual(body["current_version"], 1)

        # Replaying the same request id is idempotent.
        status, replay = self.call(
            "POST",
            "/api/reorg/submit",
            {"request_id": "r1", "manifest": swap_manifest()},
        )
        self.assertTrue(replay["replay"])

        # A second concurrent reorg is a conflict.
        status, conflict = self.call(
            "POST", "/api/reorg/submit", {"request_id": "r2", "manifest": swap_manifest()}
        )
        self.assertEqual(status, 409)
        self.assertIn("armed", conflict["error"])

        # Closing the round activates v2 atomically across all three readers.
        status, ended = self.call("POST", "/api/patrol/round/end", {"at": "2026-03-03T00:00:00Z"})
        self.assertEqual(status, 200)
        self.assertEqual(ended["current_version"], 2)
        self.assertEqual(ended["route_ids"], ["cabin-3", "cabin-1", "cabin-2"])

        status, overview = self.call("GET", "/api/env")
        self.assertEqual(overview["layout_version"], 2)
        # sensor-1 is part-1, which v2 routes to cabin-2.
        self.assertEqual(overview["sensor_1_cabin"], "cabin-2")

        status, _ = self.call("POST", "/api/reorg/confirm", {"request_id": "r1"})
        self.assertEqual(status, 200)

        # Historical alarm reviews on its birth layout; current routing drifted.
        status, review = self.call("GET", "/api/reorg/alarms")
        historical = review["alarms"][0]
        self.assertEqual(historical["birth_version"], 1)
        self.assertEqual(historical["birth_cabin"], "cabin-2")
        self.assertTrue(historical["birth_attribution_intact"])
        self.assertTrue(historical["routing_changed_since_birth"])
        self.assertEqual(historical["current_cabin"], "cabin-1")

        status, review = self.call("GET", "/api/reorg/alarms", query={"at": ["2026-03-01T00:00:00Z"]})
        self.assertEqual(review["effective_version"], 1)

    def test_immediate_effect_activates_without_round_end(self) -> None:
        status, body = self.call(
            "POST",
            "/api/reorg/submit",
            {"request_id": "imm", "manifest": swap_manifest(), "effective": "immediate"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["request"]["status"], "active")
        self.assertEqual(body["current_version"], 2)

    def test_rollback_endpoint_restores_confirmed_layout(self) -> None:
        self.call(
            "POST",
            "/api/reorg/submit",
            {"request_id": "a", "manifest": swap_manifest(), "effective": "immediate"},
        )
        self.call("POST", "/api/reorg/confirm", {"request_id": "a"})
        rotate = {
            "partitions": {"part-1": "cabin-3", "part-2": "cabin-2", "part-3": "cabin-1"},
            "route": ["cabin-2", "cabin-3", "cabin-1"],
            "sensors": {"sensor-1": "part-1", "sensor-2": "part-2", "sensor-3": "part-3"},
        }
        self.call(
            "POST",
            "/api/reorg/submit",
            {"request_id": "b", "manifest": rotate, "effective": "immediate"},
        )
        status, body = self.call("POST", "/api/reorg/rollback", {"request_id": "b"})
        self.assertEqual(status, 200)
        self.assertEqual(body["request"]["status"], "rolled_back")
        self.assertEqual(body["request"]["restored_version"], 2)
        status, snapshot = self.call("GET", "/api/reorg/snapshot")
        self.assertEqual(snapshot["partitions"], swap_manifest()["partitions"])

    def test_cancel_staged_reorg_endpoint(self) -> None:
        self.call("POST", "/api/reorg/submit", {"request_id": "s", "manifest": swap_manifest()})
        status, body = self.call("POST", "/api/reorg/rollback", {"request_id": "s"})
        self.assertEqual(status, 200)
        self.assertEqual(body["request"]["status"], "cancelled")
        self.assertEqual(body["current_version"], 1)

    def test_invalid_manifest_is_bad_request(self) -> None:
        bad = {"partitions": {"part-1": "cabin-1"}, "route": ["cabin-1"], "sensors": {}}
        status, body = self.call("POST", "/api/reorg/submit", {"request_id": "bad", "manifest": bad})
        self.assertEqual(status, 400)
        self.assertIn("drops active cabins", body["error"])

    def test_missing_request_id_is_bad_request(self) -> None:
        status, body = self.call("POST", "/api/reorg/submit", {"manifest": swap_manifest()})
        self.assertEqual(status, 400)

    def test_activate_unknown_request_is_bad_request(self) -> None:
        status, body = self.call("POST", "/api/reorg/activate", {"request_id": "ghost"})
        self.assertEqual(status, 400)

    def test_layout_history_endpoint(self) -> None:
        status, body = self.call("GET", "/api/reorg/layout", query={"version": ["1"]})
        self.assertEqual(status, 200)
        self.assertEqual(body["version"], 1)
        self.assertEqual(body["origin"], "bootstrap")
        status, body = self.call("GET", "/api/reorg/layout", query={"version": ["99"]})
        self.assertEqual(status, 404)

    def test_list_contains_all_versions_and_requests(self) -> None:
        self.call(
            "POST",
            "/api/reorg/submit",
            {"request_id": "x", "manifest": swap_manifest(), "effective": "immediate"},
        )
        status, body = self.call("GET", "/api/reorg/list")
        self.assertEqual(status, 200)
        self.assertEqual([v["version"] for v in body["versions"]], [1, 2])
        self.assertEqual([r["request_id"] for r in body["requests"]], ["x"])

    def test_legacy_partition_endpoint_is_versioned(self) -> None:
        status, body = self.call(
            "POST", "/api/env/partition", {"partition_id": "part-3", "cabin_id": "cabin-1"}
        )
        self.assertEqual(status, 200)
        self.assertGreaterEqual(body["version"], 2)
        self.assertEqual(body["revision"], 4)

    def test_patrol_rounds_endpoint(self) -> None:
        self.call("POST", "/api/patrol/round/end", {"at": "2026-03-03T00:00:00Z"})
        status, body = self.call("GET", "/api/patrol/rounds")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["rounds"]), 1)
        self.assertEqual(body["rounds"][0]["route"], ["cabin-1", "cabin-2", "cabin-3"])
        self.assertEqual(body["active_round"], 2)


if __name__ == "__main__":
    unittest.main()
