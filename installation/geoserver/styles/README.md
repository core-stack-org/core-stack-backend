# GeoServer style bundle

Bundled SLD files used by `installation/install.sh` during the `geoserver` step.

## One-time fetch from production

Run this once (with production credentials) to refresh the checked-in style bundle:

```bash
conda activate corestackenv
python installation/geoserver_style_bundle.py fetch \
  --url https://geoserver.core-stack.org:8443/geoserver \
  --username YOUR_USERNAME \
  --password YOUR_PASSWORD \
  --insecure
```

This writes:

- `*.sld` — global styles
- `workspaces/<workspace>/*.sld` — workspace-specific styles
- `manifest.json` — style index used by install-time sync

Commit the updated SLD files and `manifest.json` after verifying the fetch.

## Install-time sync

`installation/install.sh` uploads every style listed in `manifest.json` to the local GeoServer instance configured in `nrm_app/.env`.

To sync manually:

```bash
python installation/geoserver_style_bundle.py sync \
  --url http://localhost:8080/geoserver \
  --username admin \
  --password geoserver
```
