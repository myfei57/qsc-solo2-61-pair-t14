"""Partition to cabin mapping with a revision counter.

The legacy :meth:`repartition` rewrites one entry at a time.  Atomic zone
reorganisations instead use :meth:`replace_mapping`, which swaps the whole
partition table to a versioned snapshot in one step; :meth:`restore_mapping`
puts a captured snapshot back when a multi component commit has to roll back.
"""

from __future__ import annotations

import threading
from typing import Iterable


class PartitionTable:
    """Maps a partition identifier to the cabin that currently owns it.

    Every rewrite bumps a revision so the environment service can tell whether
    a cached route is still valid.
    """

    def __init__(self) -> None:
        self._mapping: dict[str, str] = {}
        self._revision = 0
        self._lock = threading.RLock()

    def repartition(self, partition_id: str, cabin_id: str) -> int:
        if not partition_id:
            raise ValueError("partition id is empty")
        if not cabin_id:
            raise ValueError("cabin id is empty")
        with self._lock:
            self._mapping[partition_id] = cabin_id
            self._revision += 1
            return self._revision

    def replace_mapping(self, mapping: dict[str, str]) -> int:
        """Atomically install a complete partition routing snapshot."""

        with self._lock:
            if any(not key or not value for key, value in mapping.items()):
                raise ValueError("partition mapping contains an empty id")
            self._mapping = dict(mapping)
            self._revision += 1
            return self._revision

    def restore_mapping(self, mapping: dict[str, str], revision: int | None = None) -> int:
        """Put a previously captured snapshot back (commit rollback).

        When ``revision`` is supplied it is restored verbatim, so the counter
        returns to the exact pre-commit value rather than advancing again.
        """

        with self._lock:
            self._mapping = dict(mapping)
            if revision is not None:
                self._revision = revision
            else:
                self._revision += 1
            return self._revision

    def cabin_of(self, partition_id: str) -> str:
        with self._lock:
            return self._mapping.get(partition_id, "")

    def partitions(self) -> Iterable[str]:
        with self._lock:
            return list(self._mapping)

    def mapping(self) -> dict[str, str]:
        with self._lock:
            return dict(self._mapping)

    def revision(self) -> int:
        with self._lock:
            return self._revision

    def describe(self) -> str:
        return f"partitions={len(self._mapping)} revision={self._revision}"
