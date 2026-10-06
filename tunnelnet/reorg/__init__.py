"""Atomic, versioned, audited zone reorganisations.

A reorg moves the partition table, the environment alarm routing and the
patrol route as one immutable :class:`ZoneLayout`.  Layouts become live at
the patrol round boundary, historical alarms stay pinned to the layout they
were born on, and rollback restores the last confirmed layout by appending a
compensating version.
"""

from .coordinator import ReorgConflict, ReorgCoordinator
from .ledger import ReorgLedger
from .manifest import normalise_manifest, validate_manifest
from .model import (
    ACTIVE_STATUSES,
    EFFECT_IMMEDIATE,
    EFFECT_ROUND_BOUNDARY,
    ORIGIN_BOOTSTRAP,
    ORIGIN_HOTFIX,
    ORIGIN_REORG,
    ORIGIN_ROLLBACK,
    STATUS_ACTIVE,
    STATUS_ARMED,
    STATUS_CANCELLED,
    STATUS_CONFIRMED,
    STATUS_FAILED,
    STATUS_ROLLED_BACK,
    ReorgRequest,
    ZoneLayout,
    canonical_payload,
    fingerprint,
)
from .report import LedgerState, ledger_state

__all__ = [
    "ACTIVE_STATUSES",
    "EFFECT_IMMEDIATE",
    "EFFECT_ROUND_BOUNDARY",
    "LedgerState",
    "ORIGIN_BOOTSTRAP",
    "ORIGIN_HOTFIX",
    "ORIGIN_REORG",
    "ORIGIN_ROLLBACK",
    "ReorgConflict",
    "ReorgCoordinator",
    "ReorgLedger",
    "ReorgRequest",
    "STATUS_ACTIVE",
    "STATUS_ARMED",
    "STATUS_CANCELLED",
    "STATUS_CONFIRMED",
    "STATUS_FAILED",
    "STATUS_ROLLED_BACK",
    "ZoneLayout",
    "canonical_payload",
    "fingerprint",
    "ledger_state",
    "normalise_manifest",
    "validate_manifest",
]
