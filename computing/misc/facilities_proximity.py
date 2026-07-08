"""Compatibility exports for the facilities local pipeline.

The runtime implementation now lives in `computing.misc.facilities.pipeline`.
This module keeps existing imports in layer dependency wiring working while
new code imports from `computing.misc.facilities`.
"""

from computing.misc.facilities.pipeline import (
    generate_facilities_proximity,
    generate_facilities_proximity_task,
    run_facilities_pipeline,
    run_facilities_request,
)

__all__ = [
    "generate_facilities_proximity",
    "generate_facilities_proximity_task",
    "run_facilities_pipeline",
    "run_facilities_request",
]

