"""Environmental readings and alarms."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Reading:
    """One sensor observation."""

    sensor_id: str
    value: float
    at: str

    def as_dict(self) -> dict[str, object]:
        return {"sensor_id": self.sensor_id, "value": self.value, "at": self.at}


@dataclass(frozen=True)
class Alarm:
    """An alarm raised from a filtered reading.

    The rearrange generation, partition and cabin are stamped when the
    alarm is raised and never rewritten, so the alarm can always be
    reviewed against the topology that was in effect at the time.
    """

    sensor_id: str
    value: float
    generation: int = 0
    partition: str = ""
    cabin: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "sensor_id": self.sensor_id,
            "value": self.value,
            "generation": self.generation,
            "partition": self.partition,
            "cabin": self.cabin,
        }
