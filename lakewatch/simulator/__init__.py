"""Offline what-if simulation for saved maintenance plans.

The simulator is a strictly read-only projection of a lake root: it loads the
current directory, manifest and snapshot state, then replays a saved plan in
memory and reports the per-action and aggregate changes in file count, bytes,
snapshot count and manifest entries.  It never imports the executor or the
ledger, and it never writes to the lake, the ledger, or the plan file.
"""
from __future__ import annotations

from .engine import (
    SIMULATION_FORMAT,
    SimulationError,
    simulate_plan,
)
from .render import render_simulation_text

__all__ = [
    "SIMULATION_FORMAT",
    "SimulationError",
    "simulate_plan",
    "render_simulation_text",
]
