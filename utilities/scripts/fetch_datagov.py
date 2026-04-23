"""
fetch_datagov.py

A robust, modular, and resumable downloader for data.gov.in resources.

FEATURES:
---------
- Resource Dictionary: Use short names (e.g., 'lgd_villages') instead of UUIDs.
- Parallel Fetching: Multi-threaded downloads for speed.
- Storage Backends: CSV, JSON, and JSONL (JSON Lines).
- In-Memory Buffering: Option to keep data in RAM and flush periodically (safer for crashes).
- Deduplication: Ensures clean restarts based on a unique key.
- Resumable: Automatically detects existing file size to resume.
- Added --ignore-total flag to force continuous downloading if API count is wrong.

USAGE:
------
The script resolves 'resource_name' from an internal dictionary.
If the name is not found, it assumes the input is a raw Resource ID (UUID).

1. Using Short Names (Fastest recommended setup):
   python fetch_datagov.py VillageSHG --api-key YOUR_KEY --format jsonl --workers 5

2. Using Raw Resource ID (Fallback):
   python fetch_datagov.py d4206736-a28b-4552-8900-7e0c23c707ac --api-key YOUR_KEY

3. In-Memory Aggregation (Process data in Python, save to disk periodically):
   python fetch_datagov.py MarketCommodityPrice --api-key YOUR_KEY \
       --in-memory --buffer-size 5000 --format jsonl --output prices.jsonl

4. Standard CSV Download (Sequential):
   python fetch_datagov.py VillageSHG --api-key YOUR_KEY --format csv

5. Force Download (If API reports wrong total count):
   python fetch_datagov.py MarketCommodityPrice --api-key YOUR_KEY --ignore-total

DEPENDENCIES:
-------------
pip install requests pandas tqdm
"""

import os
import sys
import argparse
import time
import random
import json
import threading
from typing import List, Dict, Optional, Set, Any, Union

import requests
import pandas as pd
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed, FIRST_COMPLETED
from dotenv import load_dotenv

# =============================================================================
# CONFIGURATION & CONSTANTS
# =============================================================================

BASE_URL = "https://api.data.gov.in/resource/"
DEFAULT_LIMIT = 5000
DEFAULT_TIMEOUT = 60
REQUEST_RETRIES = 3
SLEEP_BASE = 2
PAUSE_BETWEEN_REQUESTS = 0.5

# Dictionary of known resources. Add yours here.
KNOWN_RESOURCES = {
    "lgd_village":"c967fe8f-69c4-42df-8afc-8a2c98057437",
    "lgd_panchayat": "1a6c26ed-d67c-40ea-aa20-d38d35f341a5",
    "lgd_subdistrict":"6be51a29-876a-403a-a6da-42fde795e751",
    "lgd_district":"37231365-78ba-44d5-ac22-3deec40b9197",
    "lgd_state":"a71e60f0-a21d-43de-a6c5-fa5d21600cdb",
    "VillageSHG": "d4206736-a28b-4552-8900-7e0c23c707ac",
    "mgnrega": "ee03643a-ee4c-48c2-ac30-9f2ff26ab722",
    "MarketCommodityPrice": "9ef84268-d588-465a-a308-a864a43d0070",
    "RainfallIndia": "ebf28620-cb69-4737-bca3-aaeffc2c57c3",
    "DailyCommodityPrice": "35985678-0d79-46b4-9ed6-6f13308a1d24" #Variety-wise Daily Market Prices Data of Commodity
    # Add more resources as needed: "your_chosen_name": "UUID"
}

def ts() -> str:
    # Ensure datetime is imported for the ts() function
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# =============================================================================
# STORAGE HANDLERS
# =============================================================================

class BaseStorage:
    """Abstract base class for storage strategies."""
    def __init__(self, filepath: str, unique_key: Optional[str]):
        self.filepath = os.path.normpath(os.path.expanduser(filepath))
        self.unique_key = unique_key
        self.existing_count = 0
        self.seen_keys: Set[Any] = set()
        self._ensure_parent_dir()

    def _ensure_parent_dir(self):
        parent_dir = os.path.dirname(self.filepath)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

    def load(self):
        """Load existing state to determine resume point and dedup keys."""
        if os.path.exists(self.filepath):
            self._load_data()
            print(f"[{ts()}] Loaded {self.existing_count} records from existing file.")
        else:
            print(f"[{ts()}] No existing file found. Starting fresh.")

    def _load_data(self): raise NotImplementedError

    def append(self, records: List[Dict]) -> int:
        """
        Write records to storage.
        Returns the number of records actually written (after deduplication).
        """
        if not records:
            return 0

        # Deduplication Logic
        to_write = []
        if self.unique_key:
            for r in records:
                val = r.get(self.unique_key)
                if val and val in self.seen_keys:
                    continue
                if val:
                    self.seen_keys.add(val)
                to_write.append(r)
        else:
            to_write = records

        if to_write:
            self._write_batch(to_write)
        return len(to_write)

    def _write_batch(self, records: List[Dict]): raise NotImplementedError

class CsvStorage(BaseStorage):
    def _load_data(self):
        try:
            df = pd.read_csv(self.filepath, dtype=str)
            self.existing_count = len(df)
            if self.unique_key and self.unique_key in df.columns:
                self.seen_keys = set(df[self.unique_key].dropna().unique())
        except Exception as e:
            print(f"[{ts()}] Warning: Failed to read CSV {e}. Starting fresh.")

    def _write_batch(self, records: List[Dict]):
        try:
            df_new = pd.DataFrame(records)
            if not os.path.exists(self.filepath):
                df_new.to_csv(self.filepath, index=False, encoding='utf-8')
            else:
                # Append without header
                df_new.to_csv(self.filepath, mode='a', header=False, index=False, encoding='utf-8')
        except Exception as e:
            print(f"[{ts()}] Error writing CSV batch: {e}")

class JsonStorage(BaseStorage):
    def _load_data(self):
        try:
            with open(self.filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    self.existing_count = len(data)
                    if self.unique_key:
                        self.seen_keys = {item.get(self.unique_key) for item in data if self.unique_key in item}
        except Exception as e:
            print(f"[{ts()}] Warning: Failed to read JSON {e}. Starting fresh.")

    def _write_batch(self, records: List[Dict]):
        # Standard JSON requires reading the whole file to append,
        # which is slow for large datasets.
        # For robustness, we do Read-Modify-Write here.
        try:
            file_exists = os.path.exists(self.filepath)
            mode = 'r+' if file_exists else 'w+'
            with open(self.filepath, mode, encoding='utf-8', newline='\n') as f:
                data = []
                if file_exists and os.path.getsize(self.filepath) > 0:
                    try:
                        data = json.load(f)
                        if not isinstance(data, list):
                            raise ValueError("Root not a list")
                    except (json.JSONDecodeError, ValueError):
                        data = []
                data.extend(records)
                f.seek(0)
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.write("\n")
                f.truncate()
        except Exception as e:
            print(f"[{ts()}] Error writing JSON batch: {e}")

class JsonlStorage(BaseStorage):
    """JSON Lines format: One JSON object per line. Append-only."""
    def _load_data(self):
        # Must read line-by-line to check for unique keys if dedup is on
        count = 0
        if self.unique_key:
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    for line in f:
                        if not line.strip(): continue
                        count += 1
                        try:
                            obj = json.loads(line)
                            val = obj.get(self.unique_key)
                            if val: self.seen_keys.add(val)
                        except json.JSONDecodeError:
                            pass
            except Exception as e:
                print(f"[{ts()}] Warning: Failed to read JSONL {e}. Starting fresh.")
        else:
            # If no unique key, we can just check file size/line count roughly?
            # Or just assume we start appending.
            # For precise resume, we'd count lines.
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    count = sum(1 for _ in f)
            except: pass

        self.existing_count = count

    def _write_batch(self, records: List[Dict]):
        try:
            with open(self.filepath, 'a', encoding='utf-8') as f:
                for record in records:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"[{ts()}] Error writing JSONL batch: {e}")

class MemoryBufferWrapper:
    """
    Wraps any BaseStorage to provide in-memory buffering.
    Flushes to disk when buffer size is reached.
    """
    def __init__(self, storage_backend: BaseStorage, buffer_size: int):
        self.storage = storage_backend
        self.buffer_size = buffer_size
        self.buffer: List[Dict] = []
        self.buffer_lock = threading.Lock()

        # Load existing state from the underlying backend
        self.storage.load()
        self.existing_count = self.storage.existing_count

    def append(self, records: List[Dict]) -> int:
        with self.buffer_lock:
            # Dedup and add to buffer
            # Note: We run dedup logic against the storage's seen_keys set
            # The set is maintained by the storage backend object.

            filtered = []
            if self.storage.unique_key:
                for r in records:
                    val = r.get(self.storage.unique_key)
                    if val and val in self.storage.seen_keys:
                        continue
                    if val: self.storage.seen_keys.add(val)
                    filtered.append(r)
            else:
                filtered = records

            self.buffer.extend(filtered)
            if len(self.buffer) >= self.buffer_size:
                self.flush()
            return len(filtered)

    def flush(self):
        if not self.buffer:
            return
        print(f"[{ts()}] Flushing {len(self.buffer)} records to disk...")
        self.storage._write_batch(self.buffer)
        self.buffer.clear()

    def finalize(self):
        self.flush()

# =============================================================================
# API HANDLERS
# =============================================================================

def make_session(api_key: str) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "python-datagov-fetcher/3.0",
        "Accept": "application/json"
    })
    return session

def get_total_records(session: requests.Session, resource_id: str, api_key: str) -> Optional[int]:
    url = f"{BASE_URL}{resource_id}"
    # FIX: Use limit=0 to get count without fetching a data record.
    # This prevents the 'count' from being hidden inside a record object.
    params = {"api-key": api_key, "format": "json", "limit": 0, "offset": 0}
    try:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            # Fallbacks for different API response structures
            count = data.get("count") or data.get("total") or data.get("total_records")
            if isinstance(count, int):
                return count
    except Exception as e:
        print(f"[{ts()}] Failed to fetch total count: {e}")
    return None

def fetch_batch(session: requests.Session, resource_id: str, api_key: str, offset: int, limit: int) -> List[Dict]:
    url = f"{BASE_URL}{resource_id}"
    params = {"api-key": api_key, "format": "json", "limit": limit, "offset": offset}

    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            records = data.get("records") or data.get("fields", [])
            if isinstance(records, list): return records
            # Handle edge case where API returns list directly
            if isinstance(data, list): return data
            return []
        except Exception as e:
            sleep = SLEEP_BASE * attempt + random.random()
            print(f"[{ts()}] Offset {offset} failed (attempt {attempt}): {e}. Retrying in {sleep:.1f}s")
            time.sleep(sleep)
    return []

# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def resolve_resource_id(name_or_id: str) -> str:
    """Resolves a resource name to an ID, or returns the ID if it looks like a UUID."""
    if name_or_id in KNOWN_RESOURCES:
        rid = KNOWN_RESOURCES[name_or_id]
        print(f"[{ts()}] Resolved resource '{name_or_id}' to ID: {rid}")
        return rid
    # Heuristic: if it contains dashes and looks like a UUID, assume it is
    if "-" in name_or_id and len(name_or_id) > 30:
        print(f"[{ts()}] Using provided Resource ID directly: {name_or_id}")
        return name_or_id
    raise ValueError(f"Resource '{name_or_id}' not found in KNOWN_RESOURCES and doesn't look like an ID.")

def main():
    parser = argparse.ArgumentParser(
        description="Robust Data.gov.in Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Available Resources: " + ", ".join(KNOWN_RESOURCES.keys())
    )

    # Positional
    parser.add_argument("resource", help="Resource Name (e.g. VillageSHG) or Resource ID (UUID)")

    # Auth
    load_dotenv()  # this will read .env and set environment variables
    parser.add_argument("--api-key", default=os.getenv("datagov-api-key"), help="API Key (env: datagov-api-key)")

    # Output
    parser.add_argument("--output", "-o", help="Output file path (default: <resource_name>.<format>)")
    parser.add_argument("--format", choices=["csv", "json", "jsonl"], default="jsonl",
                        help="Format (default: jsonl - recommended for speed)")

    # Behavior
    parser.add_argument("--unique-key", help="Field to deduplicate by (e.g., 'sno', 'shg_id')")
    parser.add_argument("--in-memory", action="store_true",
                        help="Keep data in memory buffer and flush periodically (safer for mid-script crashes)")
    parser.add_argument("--buffer-size", type=int, default=5000,
                        help="Records to buffer before flushing to disk (default: 5000)")

    # Performance
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Records per API request")
    parser.add_argument("--workers", type=int, default=1, help="Parallel threads (default: 1)")
    parser.add_argument("--ignore-total", action="store_true", help="Ignore API total count and download until empty")

    args = parser.parse_args()

    if not args.api_key:
        sys.exit("Error: API Key missing. Set env var or use --api-key")

    # 1. Resolve Resource
    try:
        resource_id = resolve_resource_id(args.resource)
    except ValueError as e:
        sys.exit(str(e))

    # 2. Determine Output Filename
    if not args.output:
        args.output = f"{args.resource}.{args.format}"

    # 3. Setup Storage
    print(f"[{ts()}] Initializing storage: {args.format} -> {args.output}")
    if args.format == 'jsonl':
        storage_backend = JsonlStorage(args.output, args.unique_key)
    elif args.format == 'json':
        storage_backend = JsonStorage(args.output, args.unique_key)
    else: # default: csv
        storage_backend = CsvStorage(args.output, args.unique_key)


    # 4. Wrap in Memory Buffer if requested
    if args.in_memory:
        storage = MemoryBufferWrapper(storage_backend, args.buffer_size)
        print(f"[{ts()}] In-Memory Buffering enabled (Flush every {args.buffer_size} records).")
    else:
        storage = storage_backend

    # 5. Load Existing State
    storage.load()
    start_offset = storage.existing_count
    print(f"[{ts()}] Resuming from offset: {start_offset}")

    # 6. API Setup
    session = make_session(args.api_key)
    total = None
    if not args.ignore_total:
        total = get_total_records(session, resource_id, args.api_key)

    # Logic to handle bad totals (e.g. if API returns 1 but there are millions)
    if total and total < start_offset:
        print(f"[{ts()}] WARNING: API Total ({total}) is less than existing data ({start_offset}).")
        print(f"[{ts()}] Ignoring API Total and resuming.")
        total = None
    elif total:
        print(f"[{ts()}] Total records reported: {total}")
    else:
        print(f"[{ts()}] Total records unknown (or ignored). Will download until empty batch.")

    pbar = tqdm(total=total, initial=start_offset, unit="rec", desc="Downloading")

    stop_flag = False
    next_offset = start_offset
    # Max pending futures to prevent memory explosion if API is super fast
    MAX_PENDING_FUTURES = args.workers * 2

    thread_lock = threading.Lock()

    def process_batch(offset: int) -> tuple[int, List[Dict]]:
        records = fetch_batch(session, resource_id, args.api_key, offset, args.limit)
        added = storage.append(records)
        with thread_lock:
            pbar.update(added)
        return offset, records

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}

        while not stop_flag:
            # 1. Submit new tasks
            # Condition to submit: We haven't hit a known total, AND we don't have too many pending tasks
            can_submit = False
            with thread_lock:
                if total is None:
                    can_submit = True
                else:
                    if next_offset < total:
                        can_submit = True

            if can_submit and len(futures) < MAX_PENDING_FUTURES:
                # Check if we should stop because of a previous empty batch signal
                # (Logic handled inside the loop below)

                offset_to_fetch = next_offset
                next_offset += args.limit
                future = executor.submit(process_batch, offset_to_fetch)
                futures[future] = offset_to_fetch

            # 2. Wait for completion
            # Use as_completed with FIRST_COMPLETED.
            # We don't use a small timeout here because we want to wait for at least one result.
            if not futures:
                # If no futures and we can't submit (e.g. total reached or stop_flag set), break
                if not can_submit:
                    break
                # If no futures but we can submit (e.g. loop start), submit immediately (done above)
                # If still no futures after submit check, sleep briefly to avoid tight loop
                time.sleep(0.1)
                continue

            # Wait for ANY future to complete
            completed_set = as_completed(futures)

            # We need to iterate over the generator. Since we only want one (or as many as are ready),
            # we can wrap it or iterate once.
            # Using a loop to drain all currently completed futures
            for f in completed_set:
                offset = futures.pop(f)
                try:
                    fetched_offset, records = f.result()

                    if not records:
                        # If we get an empty batch, we might be done.
                        # However, if we are far ahead (parallel), an empty batch might just be a gap?
                        # data.gov usually returns empty if offset >= total.
                        print(f"[{ts()}] Empty batch received at offset {fetched_offset}. Finishing up...")
                        stop_flag = True

                except Exception as e:
                    print(f"[{ts()}] Error in future {offset}: {e}")

                # Break inner loop to check stop_flag at top of while
                break

    pbar.close()
    if hasattr(storage, 'finalize'):
        storage.finalize()

    print(f"[{ts()}] Finished. Data saved to {args.output}")

if __name__ == "__main__":
    main()
