#!/usr/bin/env python3
"""Compatibility entry point for the config-driven admin asset builder."""

from __future__ import annotations

from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utilities.scripts.admin_assets.build_admin_boundary_assets import main  # noqa: E402


if __name__ == "__main__":
    main()
