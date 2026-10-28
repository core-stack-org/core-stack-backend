"""Local GPU runoff pipeline package."""

import importlib.util
import os


def load_tests(loader, tests, pattern):
    # The subpackages import CuPy, which only the conda environment provides.
    # Without it, skip this package instead of failing test discovery.
    if importlib.util.find_spec("cupy") is None:
        return tests
    this_dir = os.path.dirname(__file__)
    project_root = os.path.dirname(os.path.dirname(this_dir))
    tests.addTests(loader.discover(this_dir, pattern or "test*.py", project_root))
    return tests
