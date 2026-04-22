#!/usr/bin/env python3
"""
Advanced ESRI Feature Layer Query Tool
--------------------------------------
A robust CLI tool to inspect, query, and extract data from ESRI MapServer
and FeatureServer endpoints.

Features:
    - Auto-pagination for large datasets.
    - CSV and GeoJSON export.
    - Metadata inspection.
    - Token-based authentication.
    - Statistics and distinct value queries.

Examples:
    # Get information about the layer
    python esri_tool.py https://sampleserver6.arcgisonline.com/arcgis/rest/services/Census/MapServer/3 info

    # List fields
    python esri_tool.py https://sampleserver6.arcgisonline.com/arcgis/rest/services/Census/MapServer/3 fields

    # Query data (limit 5, save to CSV)
    python esri_tool.py <URL> query --where "pop2000 > 100000" --limit 5 --out data.csv

    # Count features by state
    python esri_tool.py <URL> count --group-by state_name
"""

import argparse
import csv
import json
import os
import sys
import time
from urllib.parse import urlparse, urljoin

try:
    import requests
except ImportError:
    print("Error: The 'requests' library is required.")
    print("Install it via: pip install requests")
    sys.exit(1)

try:
    from pyproj import CRS, Transformer
except ImportError:
    CRS = None
    Transformer = None

# --- Configuration ---
DEFAULT_TIMEOUT = 30  # Seconds
MAX_RETRIES = 3
DEFAULT_BATCH_SIZE = 100


class ESRIError(Exception):
    """Custom exception for ESRI-specific errors."""
    pass


class ESRIClient:
    """Client for interacting with ESRI Map/Feature Services."""

    def __init__(self, endpoint, token=None, verify_ssl=True):
        self.endpoint = endpoint
        self.token = token
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        if not verify_ssl:
            self.session.verify = False
            # Suppress SSL warnings
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        # Determine if we are at a Service level or Layer level
        self.metadata = self._fetch_metadata()
        self.service_type = self.metadata.get("type", "Unknown")

        # Check if this is a Service (contains layers) or a Feature Layer
        if "layers" in self.metadata or "tables" in self.metadata:
            self.is_service = True
        else:
            self.is_service = False

    def _get_params(self, extra_params=None):
        """Merge standard params with request params."""
        params = {"f": "json"}
        if self.token:
            params["token"] = self.token
        if extra_params:
            params.update(extra_params)
        return params

    def _request(self, url, params=None):
        """Execute HTTP request with error handling."""
        params = self._get_params(params)
        
        try:
            r = self.session.get(url, params=params, timeout=2*DEFAULT_TIMEOUT)
            r.raise_for_status()
            
            # Check for ESRI JSON error
            data = r.json()
            if "error" in data:
                err_msg = data["error"].get("message", "Unknown ESRI Error")
                err_code = data["error"].get("code")
                raise ESRIError(f"ESRI Error [{err_code}]: {err_msg}")
            return data
            
        except json.JSONDecodeError:
            raise ESRIError(f"Invalid JSON response from server. Status: {r.status_code}")
        except requests.exceptions.RequestException as e:
            raise ESRIError(f"Connection failed: {e}")

    def _fetch_metadata(self):
        """Fetches the JSON description of the current endpoint."""
        return self._request(self.endpoint)

    def _layer_url(self, suffix):
        """Append an operation to the current layer endpoint without dropping the layer id."""
        return f"{self.endpoint.rstrip('/')}/{suffix.lstrip('/')}"

    def get_layers_info(self):
        """If endpoint is a Service, returns list of layers."""
        if not self.is_service:
            raise ESRIError("Endpoint is not a Service (it's likely a specific Layer).")
        
        layers = self.metadata.get("layers", [])
        tables = self.metadata.get("tables", [])
        return layers + tables

    def query(self, where="1=1", out_fields="*", geometry=None, limit=None, result_offset=0):
        """
        Execute a query. Handles pagination automatically.
        Yields features one by one or in batches if limit is set.
        """
        if self.is_service:
            raise ESRIError("Cannot run query on a Service root. Please provide a specific Layer URL (e.g. .../MapServer/0).")

        query_url = self._layer_url("query")
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "true",
            "resultOffset": result_offset
        }
        
        if geometry:
            # Simple bounding box assumption for now: xmin,ymin,xmax,ymax
            params["geometry"] = geometry
            params["inSR"] = "4326"
            params["spatialRel"] = "esriSpatialRelIntersects"

        max_record_count = self.metadata.get("maxRecordCount", 1000)
        page_size = min(max_record_count, DEFAULT_BATCH_SIZE)
        features_fetched = 0
        total_yielded = 0

        while True:
            if limit and total_yielded >= limit:
                break

            # Calculate how many to ask for in this batch
            current_batch_size = page_size
            if limit:
                remaining = limit - total_yielded
                current_batch_size = min(remaining, current_batch_size)
            
            # ArcGIS sometimes ignores resultRecordCount if it's too high, 
            # but best practice is to send it.
            # If limit is None, we want everything (pagination).
            params["resultRecordCount"] = current_batch_size

            try:
                data = self._request(query_url, params)
            except ESRIError:
                if limit:
                    raise
                yield from self._query_by_object_ids(where, out_fields, geometry, page_size)
                return
            
            features = data.get("features", [])
            if not features:
                break
            
            for feat in features:
                yield feat
                total_yielded += 1
                if limit and total_yielded >= limit:
                    return
            
            # Check for exceeded transfer limit if not using limit
            if "exceededTransferLimit" not in data and not limit:
                # Server returned all records in one go (less than max)
                break
            
            # Advance offset
            params["resultOffset"] += len(features)

    def get_object_ids(self, where="1=1"):
        """Fetch object IDs for a layer query."""
        if self.is_service:
            raise ESRIError("Cannot fetch object IDs on a Service root.")

        query_url = self._layer_url("query")
        data = self._request(query_url, {
            "where": where,
            "returnIdsOnly": "true"
        })
        return data.get("objectIdFieldName", "objectid"), data.get("objectIds", [])

    def _query_by_object_ids(self, where, out_fields, geometry, page_size):
        """Fallback query mode for layers that fail on large geometry payloads."""
        oid_field, object_ids = self.get_object_ids(where=where)
        if not object_ids:
            return

        query_url = self._layer_url("query")
        chunk_size = max(1, min(page_size, 5))

        def fetch_chunk(chunk):
            where_parts = [f"{oid_field} IN ({','.join(str(oid) for oid in chunk)})"]
            if where and where != "1=1":
                where_parts.insert(0, f"({where})")

            params = {
                "where": " AND ".join(where_parts),
                "outFields": out_fields,
                "returnGeometry": "true"
            }

            if geometry:
                params["geometry"] = geometry
                params["inSR"] = "4326"
                params["spatialRel"] = "esriSpatialRelIntersects"

            try:
                data = self._request(query_url, params)
                for feat in data.get("features", []):
                    yield feat
            except ESRIError:
                if len(chunk) == 1:
                    raise
                mid = len(chunk) // 2
                yield from fetch_chunk(chunk[:mid])
                yield from fetch_chunk(chunk[mid:])

        for i in range(0, len(object_ids), chunk_size):
            yield from fetch_chunk(object_ids[i:i + chunk_size])

    def get_unique_values(self, field):
        """Get distinct values using statistics."""
        query_url = self._layer_url("query")
        params = {
            "where": "1=1",
            "outFields": field,
            "returnDistinctValues": "true", # Newer services
            "groupByFieldsForStatistics": field, # Older services fallback
            "orderByFields": field,
            "returnGeometry": "false"
        }
        data = self._request(query_url, params)
        features = data.get("features", [])
        return [f.get("attributes", {}).get(field) for f in features]

    def get_counts(self, group_by_field=None):
        """Count total or count by grouping."""
        query_url = self._layer_url("query")
        
        if group_by_field:
            # Grouped Count
            params = {
                "where": "1=1",
                "outFields": group_by_field,
                "groupByFieldsForStatistics": group_by_field,
                "outStatistics": json.dumps([{
                    "statisticType": "count",
                    "onStatisticField": "objectid", # Most reliable field to count
                    "outStatisticFieldName": "count"
                }])
            }
        else:
            # Total Count
            # Some services require 'returnCountOnly=true' to be fast
            params = {
                "where": "1=1",
                "returnCountOnly": "true"
            }

        data = self._request(query_url, params)
        if group_by_field:
            return data.get("features", [])
        else:
            return data.get("count", 0)


# --- CLI Helpers ---

def print_table(headers, rows):
    """Simple pretty printer for tabular data."""
    if not rows:
        print("No results found.")
        return
    
    col_widths = []
    for i, header in enumerate(headers):
        max_len = len(str(header))
        for row in rows:
            val = str(row.get(header, "") if isinstance(row, dict) else row[i])
            max_len = max(max_len, len(val))
        col_widths.append(max_len + 2)

    header_row = "".join(str(headers[i]).ljust(col_widths[i]) for i in range(len(headers)))
    print(header_row)
    print("-" * len(header_row))

    for row in rows:
        row_str = "".join(str(row.get(header, "") if isinstance(row, dict) else row[i]).ljust(col_widths[i]) for i, header in enumerate(headers))
        print(row_str)

def save_to_csv(features, filename):
    """Save list of feature dicts to CSV."""
    if not features:
        print("No features to save.")
        return

    fieldnames = []
    # Extract fieldnames from first feature, flatten geometry slightly
    first_attr = features[0].get('attributes', {})
    fieldnames.extend(first_attr.keys())
    # Optionally adding a 'geometry' column as JSON string is usually enough for CSV
    fieldnames.append('geometry_geojson')

    try:
        out_dir = os.path.dirname(filename)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for feat in features:
                row = feat.get('attributes', {})
                # Add geometry as string if present
                if 'geometry' in feat:
                    row['geometry_geojson'] = json.dumps(feat['geometry'])
                writer.writerow(row)
        print(f"Successfully saved {len(features)} records to {filename}")
    except IOError as e:
        print(f"Error saving CSV: {e}")


def esri_geometry_to_geojson(geometry):
    """Convert common ESRI JSON geometry objects to GeoJSON geometry objects."""
    if not geometry:
        return None

    if "x" in geometry and "y" in geometry:
        return {
            "type": "Point",
            "coordinates": [geometry["x"], geometry["y"]]
        }

    if "points" in geometry:
        points = geometry["points"]
        if len(points) == 1:
            return {
                "type": "Point",
                "coordinates": points[0]
            }
        return {
            "type": "MultiPoint",
            "coordinates": points
        }

    if "paths" in geometry:
        paths = geometry["paths"]
        if len(paths) == 1:
            return {
                "type": "LineString",
                "coordinates": paths[0]
            }
        return {
            "type": "MultiLineString",
            "coordinates": paths
        }

    if "rings" in geometry:
        rings = geometry["rings"]
        if len(rings) == 1:
            return {
                "type": "Polygon",
                "coordinates": rings
            }
        return {
            "type": "MultiPolygon",
            "coordinates": [[ring] for ring in rings]
        }

    return None


def build_transformer(spatial_reference):
    """Build a transformer from an ESRI spatial reference object to WGS84."""
    if not spatial_reference or not CRS or not Transformer:
        return None

    source_crs = None
    wkid = spatial_reference.get("latestWkid") or spatial_reference.get("wkid")
    wkt = spatial_reference.get("wkt")

    try:
        if wkid:
            source_crs = CRS.from_epsg(wkid)
        elif wkt:
            source_crs = CRS.from_wkt(wkt)
    except Exception:
        return None

    if not source_crs:
        return None

    target_crs = CRS.from_epsg(4326)
    if source_crs == target_crs:
        return None

    return Transformer.from_crs(source_crs, target_crs, always_xy=True)


def transform_coordinates(coords, transformer):
    """Recursively transform a nested coordinate array to WGS84."""
    if not transformer:
        return coords

    if isinstance(coords[0], (int, float)):
        x, y = transformer.transform(coords[0], coords[1])
        return [x, y]

    return [transform_coordinates(part, transformer) for part in coords]


def save_to_geojson(features, filename, spatial_reference=None):
    """Save list of features to GeoJSON format."""
    transformer = build_transformer(spatial_reference)

    geojson_features = []
    for feat in features:
        geometry = esri_geometry_to_geojson(feat.get('geometry'))
        if geometry and transformer:
            geometry = {
                "type": geometry["type"],
                "coordinates": transform_coordinates(geometry["coordinates"], transformer)
            }
        geojson_feat = {
            "type": "Feature",
            "properties": feat.get('attributes', {}),
            "geometry": geometry
        }
        geojson_features.append(geojson_feat)

    geojson = {
        "type": "FeatureCollection",
        "features": geojson_features
    }

    try:
        out_dir = os.path.dirname(filename)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=2)
        print(f"Successfully saved {len(features)} features to {filename}")
    except IOError as e:
        print(f"Error saving GeoJSON: {e}")

# --- Command Functions ---

def cmd_info(args, client):
    """Display metadata."""
    if client.is_service:
        print(f"Service: {client.metadata.get('description', 'No Description')}")
        print(f"Copyright: {client.metadata.get('copyright', 'N/A')}")
        print("\nAvailable Layers:")
        layers = client.get_layers_info()
        print_table(["ID", "Name"], [{"ID": l["id"], "Name": l["name"]} for l in layers])
    else:
        print(f"Layer ID: {client.metadata.get('id')}")
        print(f"Name: {client.metadata.get('name')}")
        print(f"Description: {client.metadata.get('description', 'N/A')}")
        print(f"Geometry Type: {client.metadata.get('geometryType')}")
        print(f"Feature Count (approx): {client.metadata.get('count', 'Unknown')}")
        print(f"Max Record Count: {client.metadata.get('maxRecordCount')}")

def cmd_fields(args, client):
    """List fields."""
    if client.is_service:
        print("Error: Please specify a Layer URL to list fields (e.g. .../MapServer/0)")
        return

    fields = client.metadata.get("fields", [])
    rows = []
    for f in fields:
        rows.append({
            "Name": f['name'],
            "Type": f['type'],
            "Alias": f.get('alias', '-'),
            "Editable": f.get('editable', False)
        })
    print_table(["Name", "Type", "Alias", "Editable"], rows)

def cmd_query(args, client):
    """Run a query and display or save results."""
    print(f"Querying {args.where}...")
    start = time.time()

    try:
        # Generator for pagination
        features = list(client.query(
            where=args.where,
            out_fields=args.fields,
            limit=args.limit
        ))

        duration = time.time() - start
        print(f"Fetched {len(features)} features in {duration:.2f}s")

        if args.out:
            if args.out.endswith('.geojson'):
                spatial_reference = (
                    client.metadata.get("sourceSpatialReference")
                    or client.metadata.get("extent", {}).get("spatialReference")
                    or client.metadata.get("spatialReference")
                )
                save_to_geojson(features, args.out, spatial_reference=spatial_reference)
            elif args.out.endswith('.json'):
                out_dir = os.path.dirname(args.out)
                if out_dir:
                    os.makedirs(out_dir, exist_ok=True)
                with open(args.out, 'w') as f:
                    json.dump(features, f, indent=2)
                print(f"Saved to {args.out}")
            else:
                save_to_csv(features, args.out)
        else:
            # Print to console
            if features:
                # Print first 5 attributes
                headers = list(features[0].get('attributes', {}).keys())
                rows = [f['attributes'] for f in features[:20]] # Limit print preview
                print_table(headers, rows)
                if len(features) > 20:
                    print(f"... and {len(features) - 20} more (use --out to save all)")
    except ESRIError as e:
        print(f"Query Failed: {e}")

def cmd_count(args, client):
    """Count features."""
    if args.group_by:
        print(f"Counting by {args.group_by}...")
        features = client.get_counts(group_by_field=args.group_by)
        # Reformat attributes for display
        rows = []
        for f in features:
            attrs = f['attributes']
            # The stats field name is usually dynamic or fixed in our request
            # We asked for 'count' in outStatisticFieldName
            rows.append({"Group": attrs.get(args.group_by), "Count": attrs.get('count')})
        print_table(["Group", "Count"], rows)
    else:
        print("Counting total features...")
        count = client.get_counts()
        print(f"Total Count: {count}")

def cmd_unique(args, client):
    """Get unique values."""
    print(f"Fetching unique values for '{args.field}'...")
    values = client.get_unique_values(args.field)
    print(f"Found {len(values)} unique values:")
    for v in values:
        print(f"- {v}")

# --- Main Entry Point ---

def main():
    parser = argparse.ArgumentParser(
        description="Advanced ESRI Feature Layer Query Tool",
        epilog="Example: python esri_tool.py <URL> info"
    )
    parser.add_argument("endpoint", help="ESRI Endpoint URL (Service or Layer)")
    parser.add_argument("--token", help="Authentication token (if secured)")
    parser.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL certificate verification")

    subparsers = parser.add_subparsers(dest="command", help="Action to perform", required=True)

    # Info Command
    subparsers.add_parser("info", help="Show metadata for the service or layer")

    # Fields Command
    subparsers.add_parser("fields", help="List available fields in a layer")

    # Query Command
    parser_q = subparsers.add_parser("query", help="Query features")
    parser_q.add_argument("--where", default="1=1", help="SQL where clause")
    parser_q.add_argument("--fields", default="*", help="Comma-separated list of fields")
    parser_q.add_argument("--limit", type=int, help="Limit number of results")
    parser_q.add_argument("--out", help="Save output to file (supports .csv and .json)")

    # Count Command
    parser_c = subparsers.add_parser("count", help="Count features")
    parser_c.add_argument("--group-by", help="Field to group counts by")

    # Unique Command
    parser_u = subparsers.add_parser("unique", help="Get unique values for a field")
    parser_u.add_argument("field", help="Field name to query")

    args = parser.parse_args()

    try:
        client = ESRIClient(args.endpoint, args.token, verify_ssl=not args.no_verify_ssl)

        if args.command == "info":
            cmd_info(args, client)
        elif args.command == "fields":
            cmd_fields(args, client)
        elif args.command == "query":
            cmd_query(args, client)
        elif args.command == "count":
            cmd_count(args, client)
        elif args.command == "unique":
            cmd_unique(args, client)
            
    except ESRIError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)

if __name__ == "__main__":
    main()
