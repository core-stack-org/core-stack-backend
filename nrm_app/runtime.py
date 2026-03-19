from __future__ import annotations

import os
import sys
from pathlib import Path


_DLL_DIRECTORY_HANDLES = []
_RUNTIME_CONFIGURED = False


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def running_on_windows() -> bool:
    return os.name == "nt"


def resolve_path(value, *, base_dir: Path | None = None, default=None) -> Path:
    raw_value = value
    if raw_value in (None, ""):
        raw_value = default

    if raw_value in (None, ""):
        raise ValueError("A path value is required.")

    path = Path(os.path.expandvars(os.path.expanduser(str(raw_value))))
    if not path.is_absolute():
        path = (base_dir or project_root()) / path

    return path.resolve(strict=False)


def ensure_directory(path: Path | str) -> Path:
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _discover_conda_prefixes() -> list[Path]:
    prefixes = []
    seen = set()

    for raw_prefix in (
        os.environ.get("CONDA_PREFIX"),
        sys.prefix,
        sys.base_prefix,
    ):
        if not raw_prefix:
            continue

        prefix = Path(raw_prefix).resolve(strict=False)
        prefix_key = str(prefix)
        if prefix_key in seen:
            continue

        prefixes.append(prefix)
        seen.add(prefix_key)

    return prefixes


def _first_existing_path(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def configure_runtime_environment() -> None:
    global _RUNTIME_CONFIGURED

    if _RUNTIME_CONFIGURED:
        return

    prefixes = _discover_conda_prefixes()

    gdal_candidates = []
    proj_candidates = []
    library_candidates = []

    for prefix in prefixes:
        gdal_candidates.extend(
            [
                prefix / "Library" / "share" / "gdal",
                prefix / "share" / "gdal",
            ]
        )
        proj_candidates.extend(
            [
                prefix / "Library" / "share" / "proj",
                prefix / "share" / "proj",
            ]
        )
        library_candidates.extend(
            [
                prefix / "Library" / "bin",
                prefix / "lib",
            ]
        )

    gdal_data_dir = _first_existing_path(gdal_candidates)
    proj_dir = _first_existing_path(proj_candidates)
    library_dir = _first_existing_path(library_candidates)

    if gdal_data_dir and not os.environ.get("GDAL_DATA"):
        os.environ["GDAL_DATA"] = str(gdal_data_dir)

    if proj_dir and not os.environ.get("PROJ_LIB"):
        os.environ["PROJ_LIB"] = str(proj_dir)

    if library_dir:
        library_dir_str = str(library_dir)

        if running_on_windows() and hasattr(os, "add_dll_directory"):
            _DLL_DIRECTORY_HANDLES.append(os.add_dll_directory(library_dir_str))
        else:
            path_env = "PATH" if running_on_windows() else "LD_LIBRARY_PATH"
            existing_value = os.environ.get(path_env, "")
            existing_paths = [segment for segment in existing_value.split(os.pathsep) if segment]

            if library_dir_str not in existing_paths:
                combined_paths = [library_dir_str, *existing_paths]
                os.environ[path_env] = os.pathsep.join(combined_paths)

    _RUNTIME_CONFIGURED = True
