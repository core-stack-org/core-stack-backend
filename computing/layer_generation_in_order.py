import json
from computing.misc.admin_boundary import generate_tehsil_shape_file_data
from computing.misc.nrega import clip_nrega_district_block
from computing.mws.mws import mws_layer
from computing.mws.generate_hydrology import generate_hydrology
from computing.lulc.lulc_v3 import clip_lulc_v3
from computing.lulc.lulc_vector import vectorise_lulc
from computing.cropping_intensity.cropping_intensity import generate_cropping_intensity
from computing.surface_water_bodies.swb import generate_swb_layer
from computing.drought.drought import calculate_drought
from computing.drought.drought_causality import drought_causality
from computing.crop_grid.crop_grid import create_crop_grids
from computing.change_detection.change_detection import get_change_detection
from computing.change_detection.change_detection_vector import (
    vectorise_change_detection,
)
from computing.misc.restoration_opportunity import generate_restoration_opportunity
from computing.misc.aquifer_vector import generate_aquifer_vector
from computing.terrain_descriptor.terrain_raster import terrain_raster
from computing.terrain_descriptor.terrain_clusters import generate_terrain_clusters
from computing.lulc_X_terrain.lulc_on_plain_cluster import lulc_on_plain_cluster
from computing.lulc_X_terrain.lulc_on_slope_cluster import lulc_on_slope_cluster
from computing.misc.soge_vector import generate_soge_vector
from computing.misc.drainage_lines import clip_drainage_lines
from computing.clart.clart import generate_clart_layer
from .layer_map import *
from nrm_app.celery import app
from .models import Layer

status = {}


@app.task(bind=True)
def layer_generate_map(
    self, state, district, block, map_order, start_year=None, end_year=None
):
    """

    This function take state, district,block and map_order(map to trigger, it can be map_1, map_2, map_3, map_4). One map trigger more numbers of pipeline.

    """
    # checking:- is mws layer is generated?
    try:
        if map_order in ["map_2", "map_3", "map_4"]:
            layer = Layer.objects.get(layer_name=f"mws_{district}_{block}")
            if not layer:
                return f"check mws layer for {district}_{block}"
    except Exception as e:
        return f"exception occur while check mws for {district}_{block} as: {e}"

    global_args = {}
    if start_year:
        global_args["start_year"] = start_year
    if end_year:
        global_args["end_year"] = end_year

    # triggering map at parent level
    for func in eval(map_order):
        parent_function = func["name"]
        parent_func = globals().get(parent_function)
        args = get_args(iterator_name=func, global_args=global_args)
        deps = func.get("depends_on", [])
        run_layer_with_dependency(
            deps=deps,
            node_func_name=parent_function,
            node_func_obj=parent_func,
            state=state,
            district=district,
            block=block,
            args=args,
        )

        # triggering map at children level
        if "children" in func:
            for child in func["children"]:
                child_function = child["name"]
                child_func = globals().get(child_function)
                child_args = get_args(iterator_name=child, global_args=global_args)
                child_deps = child.get("depends_on", [])
                run_layer_with_dependency(
                    deps=child_deps,
                    node_func_name=child_function,
                    node_func_obj=child_func,
                    state=state,
                    district=district,
                    block=block,
                    args=child_args,
                )

                # triggering map at sub children level
                if "children" in child:
                    for sub_child in child["children"]:
                        sub_child_function = sub_child["name"]
                        sub_child_func = globals().get(sub_child_function)
                        sub_child_args = get_args(
                            iterator_name=sub_child, global_args=global_args
                        )
                        sub_child_deps = sub_child.get("depends_on", [])
                        run_layer_with_dependency(
                            deps=sub_child_deps,
                            node_func_name=sub_child_function,
                            node_func_obj=sub_child_func,
                            state=state,
                            district=district,
                            block=block,
                            args=sub_child_args,
                        )

    return f"{status = }"


def run_layer_with_dependency(
    deps, node_func_name, node_func_obj, state, district, block, args
):
    """

    This function checks dependency of layer if it is generated or not and call the pipeline functions and maintain status of each function,

    """
    for dep in deps:
        if dep == "clip_lulc_v3":
            l = Layer.objects.filter(layer_name__icontains=f"_{block}_level_")
            if len(l) == 21:
                status[node_func_name] = True
            else:
                status[node_func_name] = False
        if not status.get(dep, False):
            print(
                f"Skipping {node_func_name} because dependency {dep} failed or not executed."
            )
            status[node_func_name] = False
            break
    else:
        print(f"{node_func_name} is running... with args={args}, depends_on={deps}")
        try:
            result = (
                node_func_obj(state, district, block, **args)
                if args
                else node_func_obj(state, district, block)
            )
            if result:
                print(f"{node_func_name} is completed...")
                status[node_func_name] = True
            else:
                print(f"check the {node_func_name}")
                status[node_func_name] = False
        except Exception as e:
            print(f"{node_func_name} raised an error: {e}")
            status[node_func_name] = False


def get_args(iterator_name, global_args):
    """

    This function merge the global agrs and local args(define in json maps) return combination of both.

    """
    if iterator_name.get("use_global_args", False):
        args = {**global_args, **iterator_name.get("args", {})}
    else:
        args = iterator_name.get("args", {})
    return args
