"""In memory environment service behind the console.

The service keeps a view of the *current* layout version (partition routes
and sensor bindings) that the reorg coordinator installs atomically.  Every
alarm raised here is stamped with the layout version, partition and cabin in
force at raise time; later reorganisations never rewrite that birth data.
"""

from __future__ import annotations

import threading
from typing import Protocol

from tunnelnet.cabin import PartitionTable

from . import analysis
from .model import Alarm, Reading
from .report import EnvironmentState, ReadingState, latest_value


class Filterer(Protocol):
    """Anything that smooths a raw sample before it is judged."""

    def filter(self, sample: float) -> float:  # pragma: no cover - structural
        ...


class Environment:
    """Samples sensors, filters them and routes alarms to a cabin."""

    def __init__(self, filterer: Filterer, partitions: PartitionTable, threshold: float) -> None:
        self._filter = filterer
        self._partitions = partitions
        self._threshold = threshold
        self._lock = threading.RLock()
        self._sensor_partition: dict[str, str] = {}
        self._readings: dict[str, list[Reading]] = {}
        self._alarms: list[Alarm] = []
        self._layout_version = 0

    def bind_sensor_at(self, sensor_id: str, partition_id: str) -> None:
        with self._lock:
            self._sensor_partition[sensor_id] = partition_id
            self._readings.setdefault(sensor_id, [])

    def rebind_sensor(self, sensor_id: str, partition_id: str) -> None:
        """Move an existing sensor onto another partition (reorg binding)."""

        with self._lock:
            self._sensor_partition[sensor_id] = partition_id
            self._readings.setdefault(sensor_id, [])

    def sensor_partitions(self) -> dict[str, str]:
        with self._lock:
            return dict(self._sensor_partition)

    def apply_layout(
        self,
        *,
        version: int,
        partition_routes: dict[str, str],
        sensor_partitions: dict[str, str],
    ) -> None:
        """Install the routing view of one committed layout version."""

        with self._lock:
            for sensor_id, partition_id in sensor_partitions.items():
                self._sensor_partition[sensor_id] = partition_id
                self._readings.setdefault(sensor_id, [])
            self._layout_version = version

    def restore_view(
        self,
        *,
        version: int,
        sensor_partitions: dict[str, str],
    ) -> None:
        """Restore the sensor binding/version view after an aborted commit.

        This bypasses any business logic in :meth:`apply_layout` so a fault in
        a commit path cannot also defeat the rollback of that commit.
        """

        with self._lock:
            self._sensor_partition = dict(sensor_partitions)
            for sensor_id in self._sensor_partition:
                self._readings.setdefault(sensor_id, [])
            self._layout_version = version

    def layout_version(self) -> int:
        with self._lock:
            return self._layout_version

    def sample(self, sensor_id: str, value: float, at: str) -> bool:
        """Record a reading and report whether it raised an alarm."""

        with self._lock:
            self._readings.setdefault(sensor_id, []).append(
                Reading(sensor_id=sensor_id, value=value, at=at)
            )
            return self.temperature_alarm(sensor_id, value, at)

    def history(self, sensor_id: str) -> list[Reading]:
        with self._lock:
            return list(self._readings.get(sensor_id, []))

    def sensors(self) -> list[str]:
        with self._lock:
            return sorted(self._readings)

    def alarms(self) -> list[Alarm]:
        with self._lock:
            return list(self._alarms)

    def alarm_count(self) -> int:
        with self._lock:
            return len(self._alarms)

    def sample_count(self) -> int:
        with self._lock:
            return sum(len(readings) for readings in self._readings.values())

    def temperature_alarm(self, sensor_id: str, raw: float, at: str = "") -> bool:
        with self._lock:
            filtered = self._filter.filter(raw)
            if filtered < self._threshold:
                return False
            partition_id = self._sensor_partition.get(sensor_id, "")
            cabin = self._partitions.cabin_of(partition_id) if partition_id else ""
            self._alarms.append(
                Alarm(
                    sensor_id=sensor_id,
                    value=filtered,
                    at=at,
                    birth_partition=partition_id,
                    birth_cabin=cabin,
                    birth_version=self._layout_version,
                )
            )
            return True

    def threshold(self) -> float:
        return self._threshold

    def set_threshold(self, value: float) -> float:
        self._threshold = value
        return self._threshold

    def repartition(self, partition_id: str, cabin_id: str) -> int:
        return self._partitions.repartition(partition_id, cabin_id)

    def partition_of(self, sensor_id: str) -> str:
        with self._lock:
            return self._sensor_partition.get(sensor_id, "")

    def route_alarm(self, sensor_id: str) -> str:
        with self._lock:
            return self._partitions.cabin_of(self.partition_of(sensor_id))

    def values(self, sensor_id: str) -> list[float]:
        with self._lock:
            return [reading.value for reading in self._readings.get(sensor_id, [])]

    def reading_count(self, sensor_id: str) -> int:
        with self._lock:
            return len(self._readings.get(sensor_id, []))

    def average(self, sensor_id: str) -> float:
        return analysis.mean(self.values(sensor_id))

    def maximum(self, sensor_id: str) -> float:
        values = self.values(sensor_id)
        return max(values) if values else 0.0

    def minimum(self, sensor_id: str) -> float:
        values = self.values(sensor_id)
        return min(values) if values else 0.0

    def trend(self, sensor_id: str) -> float:
        values = self.values(sensor_id)
        if len(values) < 2:
            return 0.0
        return values[-1] - values[0]

    def variance(self, sensor_id: str) -> float:
        return analysis.variance(self.values(sensor_id))

    def std_dev(self, sensor_id: str) -> float:
        return analysis.standard_deviation(self.values(sensor_id))

    def percentile(self, sensor_id: str, ratio: float) -> float:
        return analysis.percentile(self.values(sensor_id), ratio)

    def state(self) -> EnvironmentState:
        sensors: dict[str, ReadingState] = {}
        with self._lock:
            for sensor_id in self.sensors():
                readings = self._readings[sensor_id]
                sensors[sensor_id] = ReadingState(
                    partition=self.partition_of(sensor_id),
                    latest=latest_value(readings),
                    count=len(readings),
                )
        return EnvironmentState(sensors=sensors, alarms=self.alarm_count(), samples=self.sample_count())

    def describe(self) -> str:
        return (
            f"environment sensors={len(self._readings)} alarms={self.alarm_count()} "
            f"threshold={self._threshold:.4f} layout=v{self._layout_version}"
        )
