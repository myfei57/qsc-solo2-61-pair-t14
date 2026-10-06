"""Environmental readings and alarms.

An alarm freezes the routing that was in force the instant it was raised
(``birth_*`` fields plus ``birth_version``).  Those values are never rewritten
when partitions are reorganised, so an alarm can always be re-checked against
the exact layout it was born on (see ``ReorgCoordinator.review_alarm``).
"""

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

    ``birth_partition`` / ``birth_cabin`` / ``birth_version`` pin the alarm to
    the layout active at raise time.  They default to empty/zero only for the
    rare alarm constructed before a coordinator exists (tests, direct use).
    """

    sensor_id: str
    value: float
    at: str = ""
    birth_partition: str = ""
    birth_cabin: str = ""
    birth_version: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "sensor_id": self.sensor_id,
            "value": self.value,
            "at": self.at,
            "birth_partition": self.birth_partition,
            "birth_cabin": self.birth_cabin,
            "birth_version": self.birth_version,
        }
