import inspect
from dataclasses import asdict, dataclass
from functools import reduce
from operator import or_
from typing import Any, Callable, Mapping

import requests
from django.conf import settings
from django.db.models import Q
from django.utils.module_loading import import_string

from computing.models import Layer
from geoadmin.models import TehsilSOI
from utilities.pipelines import api_request_payload


@dataclass(frozen=True)
class Location:
    state: str
    district: str
    block: str

    def asdict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class PipelineSpec:
    runner_path: str
    payload_builder: Callable[[Mapping[str, str], bool], dict[str, Any]]

    def run(self, location: Mapping[str, str], overwrite: bool) -> Any:
        runner = import_string(self.runner_path)
        return runner(self.payload_builder(location, overwrite))


def _standard_payload(
    location: Mapping[str, str], overwrite: bool
) -> dict[str, Any]:
    return api_request_payload(
        {
            "state": location["state"],
            "district": location["district"],
            "block": location["block"],
            "overwrite": overwrite,
        },
        overwrite=overwrite,
    )


STANDARD_PIPELINES = {
    "antyodaya": PipelineSpec(
        "computing.misc.antyodaya.run_antyodaya_request",
        _standard_payload,
    ),
    "facilities_proximity": PipelineSpec(
        "computing.misc.facilities.run_facilities_request",
        _standard_payload,
    ),
    "livestocks": PipelineSpec(
        "computing.misc.livestocks.run_livestocks_request",
        _standard_payload,
    ),
}


def _task_registry(compute: str):
    from computing.layer_dependency.layer_generation_in_order import TASK_REGISTRIES

    return TASK_REGISTRIES[compute]


def _normalize_compute(compute: str) -> str:
    value = str(compute or "local").strip().lower()
    if value not in {"gee", "local"}:
        raise ValueError("compute must be either 'gee' or 'local'")
    return value


def pipeline_names(compute: str = "local") -> tuple[str, ...]:
    compute = _normalize_compute(compute)
    names = set(_task_registry(compute))
    if compute == "local":
        names.update(STANDARD_PIPELINES)
    return tuple(sorted(names))


def get_pipeline(name: str, compute: str = "local"):
    compute = _normalize_compute(compute)
    if compute == "local" and name in STANDARD_PIPELINES:
        return STANDARD_PIPELINES[name]

    try:
        return _task_registry(compute)[name]
    except KeyError as exc:
        raise ValueError(
            f"Unknown {compute} pipeline '{name}'. Use --list-pipelines "
            "to see available pipelines."
        ) from exc


def _legacy_runner_kwargs(
    runner,
    location: Mapping[str, str],
    *,
    start_year: int | None,
    end_year: int | None,
    gee_account_id: str | None,
    overwrite: bool,
) -> dict[str, Any]:
    target = getattr(runner, "run", runner)
    parameters = inspect.signature(target).parameters
    accepts_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
    available = {
        "state": location["state"],
        "district": location["district"],
        "block": location["block"],
        "start_year": start_year,
        "end_year": end_year,
        "gee_account_id": gee_account_id,
    }
    kwargs = {
        name: value
        for name, value in available.items()
        if value is not None and (accepts_kwargs or name in parameters)
    }
    if accepts_kwargs or "overwrite" in parameters:
        kwargs["overwrite"] = overwrite
    if "is_override" in parameters:
        kwargs["is_override"] = overwrite

    missing = [
        name
        for name, parameter in parameters.items()
        if name != "self"
        and parameter.default is inspect.Parameter.empty
        and parameter.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
        and name not in kwargs
    ]
    if missing:
        raise ValueError(
            f"Pipeline requires unsupported or missing arguments: {', '.join(missing)}"
        )
    return kwargs


def validate_pipeline(
    name: str,
    *,
    compute: str = "local",
    start_year: int | None = None,
    end_year: int | None = None,
    gee_account_id: str | None = None,
    overwrite: bool = True,
) -> None:
    runner = get_pipeline(name, compute)
    if isinstance(runner, PipelineSpec):
        return
    _legacy_runner_kwargs(
        runner,
        {"state": "state", "district": "district", "block": "block"},
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
        overwrite=overwrite,
    )


def run_pipeline(
    name: str,
    location: Mapping[str, str],
    overwrite: bool = True,
    *,
    compute: str = "local",
    start_year: int | None = None,
    end_year: int | None = None,
    gee_account_id: str | None = None,
) -> Any:
    runner = get_pipeline(name, compute)
    if isinstance(runner, PipelineSpec):
        return runner.run(location, overwrite)
    kwargs = _legacy_runner_kwargs(
        runner,
        location,
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
        overwrite=overwrite,
    )
    return runner(**kwargs)


def get_active_locations(
    *,
    state: str | None = None,
    district: str | None = None,
    block: str | None = None,
    blocks: list[str] | None = None,
    limit: int | None = None,
) -> list[Location]:
    if block and blocks:
        raise ValueError("Use either block or blocks, not both.")

    queryset = (
        TehsilSOI.objects.filter(
            active_status=True,
            district__active_status=True,
            district__state__active_status=True,
        )
        .select_related("district__state")
        .order_by(
            "district__state__state_name",
            "district__district_name",
            "tehsil_name",
            "pk",
        )
    )
    if state:
        queryset = queryset.filter(district__state__state_name__iexact=state)
    if district:
        queryset = queryset.filter(district__district_name__iexact=district)
    selected_blocks = [block] if block else list(dict.fromkeys(blocks or []))
    if selected_blocks:
        queryset = queryset.filter(
            reduce(
                or_,
                (Q(tehsil_name__iexact=name) for name in selected_blocks),
            )
        )
    if limit is not None:
        queryset = queryset[:limit]

    return [
        Location(
            state=tehsil.district.state.state_name,
            district=tehsil.district.district_name,
            block=tehsil.tehsil_name,
        )
        for tehsil in queryset
    ]


def get_locally_generated_locations(
    *,
    state: str | None = None,
    district: str | None = None,
    block: str | None = None,
    blocks: list[str] | None = None,
    limit: int | None = None,
) -> list[Location]:
    if block and blocks:
        raise ValueError("Use either block or blocks, not both.")

    queryset = Layer.objects.filter(
        misc__is_generated_locally=True,
    )
    if state:
        queryset = queryset.filter(state__state_name__iexact=state)
    if district:
        queryset = queryset.filter(district__district_name__iexact=district)
    selected_blocks = [block] if block else list(dict.fromkeys(blocks or []))
    if selected_blocks:
        queryset = queryset.filter(
            reduce(
                or_,
                (Q(block__tehsil_name__iexact=name) for name in selected_blocks),
            )
        )

    locations = (
        queryset.values(
            "state__state_name",
            "district__district_name",
            "block__tehsil_name",
        )
        .order_by(
            "state__state_name",
            "district__district_name",
            "block__tehsil_name",
        )
        .distinct()
    )
    if limit is not None:
        locations = locations[:limit]

    return [
        Location(
            state=location["state__state_name"],
            district=location["district__district_name"],
            block=location["block__tehsil_name"],
        )
        for location in locations
    ]


def get_active_locations_from_api(
    *,
    state: str | None = None,
    district: str | None = None,
    block: str | None = None,
    blocks: list[str] | None = None,
    limit: int | None = None,
) -> list[Location]:
    if block and blocks:
        raise ValueError("Use either block or blocks, not both.")

    backend_url = getattr(settings, "PROD_BACKEND_URL", "").rstrip("/")
    if not backend_url:
        raise ValueError("PROD_BACKEND_URL is not configured.")

    endpoint = f"{backend_url}/api/v1/proposed_blocks/"
    try:
        response = requests.get(endpoint, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        raise ValueError(
            f"Failed to fetch active locations from {endpoint}: {exc}"
        ) from exc
    if not isinstance(payload, list):
        raise ValueError("Active locations API returned an invalid response.")

    state_filter = state.casefold() if state else None
    district_filter = district.casefold() if district else None
    selected_blocks = {
        name.casefold() for name in ([block] if block else blocks or [])
    }
    locations = []

    try:
        for state_data in payload:
            state_name = state_data["label"]
            if state_filter and state_name.casefold() != state_filter:
                continue
            for district_data in state_data["district"]:
                district_name = district_data["label"]
                if (
                    district_filter
                    and district_name.casefold() != district_filter
                ):
                    continue
                for block_data in district_data["blocks"]:
                    block_name = block_data["label"]
                    if (
                        selected_blocks
                        and block_name.casefold() not in selected_blocks
                    ):
                        continue
                    locations.append(
                        Location(state_name, district_name, block_name)
                    )
                    if limit is not None and len(locations) >= limit:
                        return locations
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError(
            "Active locations API returned an invalid response."
        ) from exc

    return locations
