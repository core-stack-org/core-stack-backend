"""Minimal tests for the GEE landslide susceptibility scaffold."""

def test_default_weights_present():
    from gee_kyl import process_landslide_susceptibility as pls

    assert hasattr(pls, "DEFAULT_WEIGHTS")
    keys = set(pls.DEFAULT_WEIGHTS.keys())
    assert {"slope", "curvature", "flow_acc", "lulc", "rainfall"}.issubset(keys)
