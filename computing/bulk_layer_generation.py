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
    dataset_names: tuple[str, ...]

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
        ("Antyodaya 2020",),
    ),
    "facilities_proximity": PipelineSpec(
        "computing.misc.facilities.run_facilities_request",
        _standard_payload,
        ("Facilities Points", "Facilities Proximity"),
    ),
    "livestocks": PipelineSpec(
        "computing.misc.livestocks.run_livestocks_request",
        _standard_payload,
        ("Livestock Census 2019",),
    ),
}

LOCAL_TASK_DATASETS = {
    "computing.misc.nrega_local_compute.generate_nrega_data_local": ("NREGA Assets",),
    "computing.lulc.lulc_v3_local.clip_lulc_v3": ("LULC_v3", "LULC_level_3"),
    "computing.lulc.lulc_vector_local.vectorise_lulc": ("LULC",),
    "computing.cropping_intensity.cropping_intesity_local.generate_cropping_intensity": (
        "Cropping Intensity",
    ),
    "computing.surface_water_bodies.swb_local.generate_swb_layer": (
        "Surface Water Bodies",
    ),
    "computing.change_detection.change_detection_local.get_change_detection": (
        "Change Detection Raster",
    ),
    "computing.change_detection.change_detection_vector_local.vectorise_change_detection": (
        "Change Detection Vector",
    ),
    "computing.misc.aquifer_vector_local.generate_aquifer_vector": ("Aquifer",),
    "computing.terrain_descriptor.terrain_raster_fabdem_local.generate_terrain_raster_clip": (
        "Terrain Raster",
    ),
    "computing.terrain_descriptor.terrain_clusters_local.generate_terrain_clusters": (
        "Terrain Vector",
    ),
    "computing.terrain_descriptor.terrain_compute_all_local.generate_terrain_compute_all": (
        "Terrain Raster",
        "Terrain Vector",
    ),
    "computing.lulc_X_terrain.lulc_on_plain_cluster_local.lulc_on_plain_cluster_local": (
        "Terrain LULC",
    ),
    "computing.lulc_X_terrain.lulc_on_slope_cluster_local.lulc_on_slope_cluster_local": (
        "Terrain LULC",
    ),
    "computing.misc.soge_vector_local_compute.generate_soge_vector_local": ("SOGE",),
    "computing.misc.drainage_lines_local_compute.clip_drainage_lines": ("Drainage",),
    "computing.misc.naturaldepression_local_compute.generate_natural_depression_data_local": (
        "Natural Depression",
    ),
    "computing.misc.distancetonearestdrainage_local_compute.generate_distance_to_nearest_drainage_line_local": (
        "Distance to Drainage Line",
    ),
    "computing.misc.catchment_area_local_compute.generate_catchment_area_singleflow_local": (
        "Catchment Area",
    ),
    "computing.misc.slope_percentage_local_compute.generate_slope_percentage_data_local": (
        "Slope Percentage",
    ),
    "computing.misc.lcw_conflict_local_compute.generate_lcw_conflict_data_local": (
        "LCW Conflict",
    ),
    "computing.misc.agroecological_space_local_compute.generate_agroecological_data_local": (
        "Agroecological",
    ),
    "computing.misc.factory_csr_local_compute.generate_factory_csr_data_local": (
        "Factory CSR",
    ),
    "computing.misc.green_credit_local_compute.generate_green_credit_data_local": (
        "Green Credit",
    ),
    "computing.misc.mining_data_local_compute.generate_mining_data_local": ("Mining",),
    "computing.misc.restoration_opportunity_local_compute.generate_restoration_opportunity_local": (
        "Restoration Raster",
        "Restoration Vector",
    ),
    "computing.mws.mws_connectivity_local_compute.mws_connectivity_vector": (
        "Mws Connectivity",
    ),
    "computing.misc.facilities_proximity_local_compute.generate_facilities_proximity_local": (
        "Facilities Proximity",
    ),
    "computing.misc.livestocks_local_compute.generate_livestocks_data_local": (
        "Livestock Census 2019",
    ),
    "computing.misc.antyodaya_local_compute.generate_antyodaya_data_local": (
        "Antyodaya 2020",
    ),
    "computing.misc.drainage_density_local_compute.drainage_density": (
        "Drainage Density Vector",
    ),
    "computing.misc.river_local_compute.river_vector": ("River Vector",),
    "computing.misc.canal_local_compute.canal_vector": ("Canal Vector",),
    "computing.misc.digital_elevation_model_local.generate_febdem_raster_vector_clip": (
        "DEM Raster",
        "DEM Vector",
    ),
    "computing.mws.mws_centroid_local_compute.generate_mws_centroid_data_local": (
        "Mws Centroid",
    ),
    "computing.tree_health.local.canopy_height_local.tree_health_ch_raster_local": (
        "Canopy Height Raster",
    ),
    "computing.tree_health.local.canopy_height_vector_local.tree_health_ch_vector_local": (
        "Canopy Height Vector",
    ),
    "computing.tree_health.local.ccd_local.tree_health_ccd_raster_local": (
        "Ccd Raster",
    ),
    "computing.tree_health.local.ccd_vector_local.tree_health_ccd_vector_local": (
        "Ccd Vector",
    ),
    "computing.tree_health.local.overall_change_local.tree_health_overall_change_raster_local": (
        "Tree Overall Change Raster",
    ),
    "computing.tree_health.local.overall_change_vector_local.tree_health_overall_change_vector_local": (
        "Tree Overall Change Vector",
    ),
    "computing.soil_health.soil_health.soil_health_local": (
        "Soil Health Raster",
        "Soil Health Vector",
    ),
    "computing.soil_type.soil_type_local.generate_soil_type_local": ("Soil Type",),
    "computing.zoi_layers.zoi.generate_zoi": ("Surface Water Bodies",),
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


def get_regeneration_dataset_names(name: str) -> tuple[str, ...]:
    pipeline = get_pipeline(name, compute="local")
    if isinstance(pipeline, PipelineSpec):
        return pipeline.dataset_names

    try:
        return LOCAL_TASK_DATASETS[pipeline.name]
    except KeyError as exc:
        raise ValueError(
            f"Local regeneration datasets are not configured for pipeline '{name}'."
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
    dataset_names: tuple[str, ...],
    state: str | None = None,
    district: str | None = None,
    block: str | None = None,
    blocks: list[str] | None = None,
    limit: int | None = None,
) -> list[Location]:
    if block and blocks:
        raise ValueError("Use either block or blocks, not both.")

    queryset = Layer.objects.filter(
        dataset__name__in=dataset_names,
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
