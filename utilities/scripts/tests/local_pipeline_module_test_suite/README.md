# Local Pipeline Module Test Suite

Focused tests for the shared local pipeline contract, Unicode normalization,
GeoPackage multi-layer publishing, and GeoLibre project generation.

Run from the repository root:

```bash
PROJ_LIB=/usr/share/proj \
/home/amitportal/miniconda3/envs/corestack-backend/bin/python -m unittest discover \
  -s utilities/scripts/tests/local_pipeline_module_test_suite \
  -p 'test_*.py' -v
```

`geoserver_publish_smoke.py` is the optional live test. It supports either one
layer (`--layer-name`) or repeated `--layer PUBLISHED=SOURCE` mappings for a
multi-layer GeoPackage. Use only the configured test workspace.
