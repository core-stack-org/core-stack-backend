# Admin boundary asset configs

These JSON files define reproducible property joins against the canonical
sanitised village boundary layer.

Validate a config without writing outputs:

```bash
python utilities/scripts/build_admin_boundary_assets.py \
  asset antyodaya --validate-only
```

Build both GeoPackage and GeoJSON outputs:

```bash
python utilities/scripts/build_admin_boundary_assets.py \
  asset antyodaya --overwrite

python utilities/scripts/build_admin_boundary_assets.py \
  asset livestock --overwrite
```

The configured `admin_columns` are written first. When
`include_joined_columns` is true, selected source columns follow in their
original source-file order. This avoids maintaining a second, manually sorted
list of hundreds of Antyodaya fields.

Large CSV inputs can set `processing.join_storage` to `sqlite`. The builder
indexes the selected properties in a temporary SQLite database and only loads
the keys needed for each geometry chunk. Smaller inputs can use `memory`.

Config paths are repository-relative unless absolute paths are supplied.
