"""Dependency-free pieces shared by stats layer sources (importable without Django)."""


class LayerUnavailable(Exception):
    """The active layer source has no data for this layer (skip, not fail)."""
