"""Append only, file backed ledger of layout versions and reorg requests.

Every layout version and every reorg request is one immutable JSON document.
A single ``current`` pointer names the live layout; the pointer is written
last so an interrupted commit can never publish a layout whose documents did
not all reach disk.  Reads reconstruct the whole ledger on demand; the in
memory coordinator remains the hot path, this ledger is its durable audit.
"""

from __future__ import annotations

import threading

from tunnelnet.store import Store

from .model import ReorgRequest, ZoneLayout

LAYOUT_KIND = "zone_layout"
REQUEST_KIND = "reorg_request"
POINTER_KIND = "zone_pointer"
POINTER_ID = "current"


class ReorgLedger:
    """Persists versions and requests without ever overwriting a version."""

    def __init__(self, store: Store) -> None:
        self._store = store
        self._lock = threading.RLock()

    @property
    def store(self) -> Store:
        return self._store

    def save_layout(self, layout: ZoneLayout) -> None:
        with self._lock:
            self._store.save(LAYOUT_KIND, str(layout.version), layout.as_dict())

    def load_layout(self, version: int) -> ZoneLayout | None:
        with self._lock:
            payload = self._store.load(LAYOUT_KIND, str(version))
            return None if payload is None else ZoneLayout.from_dict(payload)

    def layouts(self) -> list[ZoneLayout]:
        with self._lock:
            result = []
            for record_id in self._store.list(LAYOUT_KIND):
                payload = self._store.load(LAYOUT_KIND, record_id)
                if payload is not None:
                    result.append(ZoneLayout.from_dict(payload))
            result.sort(key=lambda layout: layout.version)
            return result

    def save_request(self, request: ReorgRequest) -> None:
        with self._lock:
            self._store.save(REQUEST_KIND, request.request_id, request.as_dict())

    def load_request(self, request_id: str) -> ReorgRequest | None:
        with self._lock:
            payload = self._store.load(REQUEST_KIND, request_id)
            return None if payload is None else ReorgRequest.from_dict(payload)

    def requests(self) -> list[ReorgRequest]:
        with self._lock:
            result = []
            for record_id in self._store.list(REQUEST_KIND):
                payload = self._store.load(REQUEST_KIND, record_id)
                if payload is not None:
                    result.append(ReorgRequest.from_dict(payload))
            result.sort(key=lambda request: (request.created_at, request.request_id))
            return result

    def set_current(self, version: int) -> None:
        """Publish the live pointer; called only after every layout is durable."""

        with self._lock:
            self._store.save(POINTER_KIND, POINTER_ID, {"version": version})

    def current_version(self) -> int:
        with self._lock:
            payload = self._store.load(POINTER_KIND, POINTER_ID)
            if not payload:
                return 0
            try:
                return int(payload.get("version", 0) or 0)
            except (TypeError, ValueError):
                return 0
