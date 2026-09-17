"""
Auto-generated STACD DAG: STACD Local Compute DAG - Dynamic Layers (v1)
Generated at: 2026-09-10T08:39:28.596918
Description: Local-compute pipeline for dynamic layers (yearly/derived layers with inter-dependencies) from local_layer_map.json.

Supports:
- fullexec: Run all root algorithms (and their downstream automatically)
- update_algo: Run from updated algorithm downstream
- update_dataset: Run from dataset consumers downstream

This DAG is FULLY GENERIC - all logic derived from YAML configuration.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import sys
import os

# Add database path
AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/database'))
from db_operations import STACDDatabase

# ========== Configuration ==========
DAG_ID = "CoreStack_DAG_LocalCompute_DynamicLayers"
GROUP = "corestack"
DB_PATH = os.path.join(AIRFLOW_HOME_RT, 'stacd/database/stacd_database.db')

# ========== Dependency Graph (for reference) ==========
dependencies = {
    "NREGA_Clip": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "NREGA_Layer"
        ]
    },
    "LULC_Algorithm": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "LULC_Raster"
        ]
    },
    "LULC_Vectorization": {
        "inputs": [
            "LULC_Raster"
        ],
        "outputs": [
            "LULC_Vector"
        ]
    },
    "Change_Detection": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Change_Detection_Asset"
        ]
    },
    "Change_Detection_Vector": {
        "inputs": [
            "Change_Detection_Asset"
        ],
        "outputs": [
            "Change_Detection_Vector_Asset"
        ]
    },
    "Terrain_Algorithm": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Terrain_Raster"
        ]
    },
    "Terrain_Clusters": {
        "inputs": [
            "Terrain_Raster"
        ],
        "outputs": [
            "Terrain_Vector"
        ]
    },
    "LULC_Terrain_Plain": {
        "inputs": [
            "Terrain_Raster",
            "LULC_Raster"
        ],
        "outputs": [
            "LULC_Terrain_Plain_Asset"
        ]
    },
    "LULC_Terrain_Slope": {
        "inputs": [
            "Terrain_Raster",
            "LULC_Raster"
        ],
        "outputs": [
            "LULC_Terrain_Slope_Asset"
        ]
    },
    "Cropping_Intensity": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Cropping_Intensity_Asset"
        ]
    },
    "SWB_Layer": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "SWB_Layer_Asset"
        ]
    },
    "ZOI": {
        "inputs": [
            "SWB_Layer_Asset"
        ],
        "outputs": [
            "ZOI_Asset"
        ]
    },
    "Tree_Health_CH_Raster": {
        "inputs": [
            "LULC_Raster"
        ],
        "outputs": [
            "Tree_Health_CH_Raster_Asset"
        ]
    },
    "Tree_Health_CH_Vector": {
        "inputs": [
            "Tree_Health_CH_Raster_Asset"
        ],
        "outputs": [
            "Tree_Health_CH_Vector_Asset"
        ]
    },
    "Tree_Health_CCD_Raster": {
        "inputs": [
            "LULC_Raster"
        ],
        "outputs": [
            "Tree_Health_CCD_Raster_Asset"
        ]
    },
    "Tree_Health_CCD_Vector": {
        "inputs": [
            "Tree_Health_CCD_Raster_Asset"
        ],
        "outputs": [
            "Tree_Health_CCD_Vector_Asset"
        ]
    },
    "Tree_Health_OC_Raster": {
        "inputs": [
            "Change_Detection_Asset"
        ],
        "outputs": [
            "Tree_Health_OC_Raster_Asset"
        ]
    },
    "Tree_Health_OC_Vector": {
        "inputs": [
            "Tree_Health_OC_Raster_Asset"
        ],
        "outputs": [
            "Tree_Health_OC_Vector_Asset"
        ]
    },
    "Soil_Health": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Soil_Health_Asset"
        ]
    }
}

# ========== Helper Functions ==========

def generate_stac_for_dataset(dataset_instance, params, api_response):
    """Generate STAC item for a dataset instance"""
    import sys
    import os
    sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
    
    from stac_builder import write_items_from_api_response
    from config import CATALOG_OUTPUT_DIR
    
    state = params.get('state', 'unknown')
    district = params.get('district', 'unknown')
    block = params.get('block', 'unknown')
    
    output_dir = CATALOG_OUTPUT_DIR / "datasets" / "corestack" / state / district / block
    
    if api_response:
        written_files = write_items_from_api_response(
            api_response=api_response,
            output_dir=output_dir,
            extra_fields={'stacd:run_id': dataset_instance.run_id, 'stacd:dag_id': DAG_ID}
        )
        for f in written_files:
            print("STAC item written: " + str(f))
    else:
        print("No API response provided to generate STAC items.")

    
def augment_and_write_stac(stac_spec, dataset_type_id, instance_id, params):
    """Write the algo-returned stac_spec (already augmented) to the catalog"""
    import sys
    import os
    import json
    sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
    from config import CATALOG_OUTPUT_DIR

    state = params.get('state', 'unknown')
    district = params.get('district', 'unknown')
    block = params.get('block', 'unknown')

    output_dir = CATALOG_OUTPUT_DIR / "datasets" / state / district / block
    output_dir.mkdir(parents=True, exist_ok=True)

    item_filename = f"{dataset_type_id}_{instance_id}.json"
    item_path = output_dir / item_filename

    with open(item_path, 'w') as f:
        json.dump(stac_spec, f, indent=2, ensure_ascii=False)
    print(f"STAC item written: {item_path}")





def get_active_algorithm_config(algo_type_id):
    """Query database for active algorithm version config"""
    db = STACDDatabase(DB_PATH)
    try:
        algo_instance = db.get_active_algorithm_version(algo_type_id)
        if not algo_instance:
            raise ValueError(f"No active version found for {algo_type_id}")
        
        print(f"Using {algo_type_id} version {algo_instance.version}")
        print(f"   Execution modes: {algo_instance.execution_modes}")
        
        return {
            'version': algo_instance.version,
            'execution_modes': algo_instance.execution_modes
        }
    finally:
        db.close()

def determine_execution_path(**context):
    """
    Determine which tasks to run based on execution_type
    - fullexec: Return list of all root algorithms
    - update_algo: Return updated algorithm only
    - update_dataset: Return algorithms consuming updated dataset
    """
    params = context['params']
    execution_type = params.get('execution_type', 'fullexec')
    
    print(f"🔀 Execution type: {execution_type}")

    if execution_type == 'fullexec':
        # Run all root datasets AND root algorithms
        root_datasets = ['MWS_Boundaries']
        root_algos = ['NREGA_Clip', 'LULC_Algorithm', 'Change_Detection', 'Terrain_Algorithm', 'Cropping_Intensity', 'SWB_Layer', 'Soil_Health']
        entry_points = root_datasets + root_algos
        print(f"📌 Running root datasets: {root_datasets}")
        print(f"📌 Running root algorithms: {root_algos}")
        return entry_points
    
    elif execution_type == 'update_algo':
        updated_algo = params.get('updated_algo')
        if not updated_algo:
            raise ValueError("updated_algo parameter required for update_algo execution")
        
        print(f"📌 Running from updated algorithm: {updated_algo}")
        return [updated_algo]
    
    elif execution_type == 'update_dataset':
        updated_dataset = params.get('updated_dataset')
        if not updated_dataset:
            raise ValueError("updated_dataset parameter required for update_dataset execution")
        
        # Find algorithms that consume this dataset
        consumers = []
        for algo_id, deps in dependencies.items():
            if updated_dataset in deps['inputs']:
                consumers.append(algo_id)
        
        print(f"📌 Dataset {updated_dataset} consumed by: {consumers}")
        print(f"📌 Running: [{updated_dataset}] → {consumers}")

        return [updated_dataset] + consumers if consumers else [updated_dataset]



    elif execution_type == 'update_dag':
        # Run ONLY the newly added algo nodes
        # These are nodes present in current DAG structure but
        # not previously run (i.e. no successful execution in DB)
        db = STACDDatabase(DB_PATH)
        try:
            dag_record = db.get_dag_by_id(DAG_ID)
            if not dag_record:
                raise ValueError(f"DAG {DAG_ID} not found in DB")

            # Get all algos that have NEVER had a successful execution for this region
            all_algo_ids = list(dependencies.keys())
            new_algos = []
            for algo_id in all_algo_ids:
                execution = db.get_algorithm_execution(
                    algo_type_id=algo_id
                )
                if not execution or execution.status != 'success':
                    new_algos.append(algo_id)

            if not new_algos:
                raise ValueError("No new algo nodes found. All algos have prior successful executions.")

        finally:
            db.close()

        print(f" New algo nodes to run: {new_algos}")
        return new_algos



        

    elif execution_type == 'resume_exec':
        db = STACDDatabase(DB_PATH)
        try:
            failed_algos = db.get_failed_executions(
                dag_id=DAG_ID,
                state=params.get('state'),
                district=params.get('district'),
                block=params.get('block'),
                start_year=params.get('start_year'),
                end_year=params.get('end_year')
            )
        finally:
            db.close()

        if not failed_algos:
            raise ValueError("No failed executions found matching these parameters. Use fullexec to run from scratch.")

        print(f" Resuming from {len(failed_algos)} failed task(s): {failed_algos}")
        return failed_algos

    
    else:
        raise ValueError(f"Unknown execution_type: {execution_type}")

def log_algo_failure(context):
    """Called automatically by Airflow when an algo task fails"""
    ti = context['ti']
    run_id = context['run_id']
    params = context.get('params', {})
    algo_type_id = ti.task_id

    execution_params = {
        'state': params.get('state'),
        'district': params.get('district'),
        'block': params.get('block'),
        'start_year': params.get('start_year'),
        'end_year': params.get('end_year'),
        'execution_id': run_id
    }

    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        if dag_record:
            db.log_algorithm_execution(
                dag_uuid=dag_record.dag_uuid,
                algo_type_id=algo_type_id,
                version='unknown',
                run_id=run_id,
                execution_params=execution_params,
                output_ref=None,
                status='failed'
            )
            print(f"Logged failure for {algo_type_id} run_id={run_id}")
    except Exception as e:
        print(f"Could not log failure: {e}")
    finally:
        db.close()


# ========== Algorithm Task Functions ==========

def execute_NREGA_Clip(**context):
    """
    Execute NREGA_Clip - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: NREGA_Clip")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    MWS_Boundaries_asset_id = ti.xcom_pull(task_ids='MWS_Boundaries', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('NREGA_Clip')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'MWS_Boundaries': MWS_Boundaries_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for NREGA_Clip")
    
    print(f"✓ NREGA_Clip completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='NREGA_Clip',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_LULC_Algorithm(**context):
    """
    Execute LULC_Algorithm - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: LULC_Algorithm")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    MWS_Boundaries_asset_id = ti.xcom_pull(task_ids='MWS_Boundaries', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('LULC_Algorithm')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'MWS_Boundaries': MWS_Boundaries_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for LULC_Algorithm")
    
    print(f"✓ LULC_Algorithm completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='LULC_Algorithm',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_LULC_Vectorization(**context):
    """
    Execute LULC_Vectorization - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: LULC_Vectorization")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    LULC_Raster_asset_id = ti.xcom_pull(task_ids='LULC_Raster', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('LULC_Vectorization')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'LULC_Raster': LULC_Raster_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for LULC_Vectorization")
    
    print(f"✓ LULC_Vectorization completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='LULC_Vectorization',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Change_Detection(**context):
    """
    Execute Change_Detection - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Change_Detection")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    MWS_Boundaries_asset_id = ti.xcom_pull(task_ids='MWS_Boundaries', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Change_Detection')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'MWS_Boundaries': MWS_Boundaries_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Change_Detection")
    
    print(f"✓ Change_Detection completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Change_Detection',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Change_Detection_Vector(**context):
    """
    Execute Change_Detection_Vector - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Change_Detection_Vector")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Change_Detection_Asset_asset_id = ti.xcom_pull(task_ids='Change_Detection_Asset', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Change_Detection_Vector')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'Change_Detection_Asset': Change_Detection_Asset_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Change_Detection_Vector")
    
    print(f"✓ Change_Detection_Vector completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Change_Detection_Vector',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Terrain_Algorithm(**context):
    """
    Execute Terrain_Algorithm - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Terrain_Algorithm")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    MWS_Boundaries_asset_id = ti.xcom_pull(task_ids='MWS_Boundaries', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Terrain_Algorithm')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'MWS_Boundaries': MWS_Boundaries_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Terrain_Algorithm")
    
    print(f"✓ Terrain_Algorithm completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Terrain_Algorithm',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Terrain_Clusters(**context):
    """
    Execute Terrain_Clusters - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Terrain_Clusters")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Terrain_Raster_asset_id = ti.xcom_pull(task_ids='Terrain_Raster', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Terrain_Clusters')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'Terrain_Raster': Terrain_Raster_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Terrain_Clusters")
    
    print(f"✓ Terrain_Clusters completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Terrain_Clusters',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_LULC_Terrain_Plain(**context):
    """
    Execute LULC_Terrain_Plain - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: LULC_Terrain_Plain")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Terrain_Raster_asset_id = ti.xcom_pull(task_ids='Terrain_Raster', key='asset_id')
    LULC_Raster_asset_id = ti.xcom_pull(task_ids='LULC_Raster', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('LULC_Terrain_Plain')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'Terrain_Raster': Terrain_Raster_asset_id,
        'LULC_Raster': LULC_Raster_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for LULC_Terrain_Plain")
    
    print(f"✓ LULC_Terrain_Plain completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='LULC_Terrain_Plain',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_LULC_Terrain_Slope(**context):
    """
    Execute LULC_Terrain_Slope - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: LULC_Terrain_Slope")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Terrain_Raster_asset_id = ti.xcom_pull(task_ids='Terrain_Raster', key='asset_id')
    LULC_Raster_asset_id = ti.xcom_pull(task_ids='LULC_Raster', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('LULC_Terrain_Slope')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'Terrain_Raster': Terrain_Raster_asset_id,
        'LULC_Raster': LULC_Raster_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for LULC_Terrain_Slope")
    
    print(f"✓ LULC_Terrain_Slope completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='LULC_Terrain_Slope',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Cropping_Intensity(**context):
    """
    Execute Cropping_Intensity - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Cropping_Intensity")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    MWS_Boundaries_asset_id = ti.xcom_pull(task_ids='MWS_Boundaries', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Cropping_Intensity')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'MWS_Boundaries': MWS_Boundaries_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Cropping_Intensity")
    
    print(f"✓ Cropping_Intensity completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Cropping_Intensity',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_SWB_Layer(**context):
    """
    Execute SWB_Layer - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: SWB_Layer")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    MWS_Boundaries_asset_id = ti.xcom_pull(task_ids='MWS_Boundaries', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))
    gee_account_id = int(params.get('gee_account_id'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('SWB_Layer')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'gee_account_id': gee_account_id,
        'MWS_Boundaries': MWS_Boundaries_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for SWB_Layer")
    
    print(f"✓ SWB_Layer completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='SWB_Layer',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_ZOI(**context):
    """
    Execute ZOI - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: ZOI")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    SWB_Layer_Asset_asset_id = ti.xcom_pull(task_ids='SWB_Layer_Asset', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    gee_account_id = int(params.get('gee_account_id'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('ZOI')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'gee_account_id': gee_account_id,
        'SWB_Layer_Asset': SWB_Layer_Asset_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for ZOI")
    
    print(f"✓ ZOI completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='ZOI',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Tree_Health_CH_Raster(**context):
    """
    Execute Tree_Health_CH_Raster - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Tree_Health_CH_Raster")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    LULC_Raster_asset_id = ti.xcom_pull(task_ids='LULC_Raster', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Tree_Health_CH_Raster')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'LULC_Raster': LULC_Raster_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Tree_Health_CH_Raster")
    
    print(f"✓ Tree_Health_CH_Raster completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Tree_Health_CH_Raster',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Tree_Health_CH_Vector(**context):
    """
    Execute Tree_Health_CH_Vector - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Tree_Health_CH_Vector")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Tree_Health_CH_Raster_Asset_asset_id = ti.xcom_pull(task_ids='Tree_Health_CH_Raster_Asset', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Tree_Health_CH_Vector')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'Tree_Health_CH_Raster_Asset': Tree_Health_CH_Raster_Asset_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Tree_Health_CH_Vector")
    
    print(f"✓ Tree_Health_CH_Vector completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Tree_Health_CH_Vector',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Tree_Health_CCD_Raster(**context):
    """
    Execute Tree_Health_CCD_Raster - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Tree_Health_CCD_Raster")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    LULC_Raster_asset_id = ti.xcom_pull(task_ids='LULC_Raster', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Tree_Health_CCD_Raster')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'LULC_Raster': LULC_Raster_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Tree_Health_CCD_Raster")
    
    print(f"✓ Tree_Health_CCD_Raster completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Tree_Health_CCD_Raster',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Tree_Health_CCD_Vector(**context):
    """
    Execute Tree_Health_CCD_Vector - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Tree_Health_CCD_Vector")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Tree_Health_CCD_Raster_Asset_asset_id = ti.xcom_pull(task_ids='Tree_Health_CCD_Raster_Asset', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Tree_Health_CCD_Vector')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'Tree_Health_CCD_Raster_Asset': Tree_Health_CCD_Raster_Asset_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Tree_Health_CCD_Vector")
    
    print(f"✓ Tree_Health_CCD_Vector completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Tree_Health_CCD_Vector',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Tree_Health_OC_Raster(**context):
    """
    Execute Tree_Health_OC_Raster - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Tree_Health_OC_Raster")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Change_Detection_Asset_asset_id = ti.xcom_pull(task_ids='Change_Detection_Asset', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')
    start_year = int(params.get('start_year'))
    end_year = int(params.get('end_year'))

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Tree_Health_OC_Raster')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'start_year': start_year,
        'end_year': end_year,
        'Change_Detection_Asset': Change_Detection_Asset_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Tree_Health_OC_Raster")
    
    print(f"✓ Tree_Health_OC_Raster completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Tree_Health_OC_Raster',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Tree_Health_OC_Vector(**context):
    """
    Execute Tree_Health_OC_Vector - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Tree_Health_OC_Vector")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Tree_Health_OC_Raster_Asset_asset_id = ti.xcom_pull(task_ids='Tree_Health_OC_Raster_Asset', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Tree_Health_OC_Vector')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'Tree_Health_OC_Raster_Asset': Tree_Health_OC_Raster_Asset_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Tree_Health_OC_Vector")
    
    print(f"✓ Tree_Health_OC_Vector completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Tree_Health_OC_Vector',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


def execute_Soil_Health(**context):
    """
    Execute Soil_Health - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Soil_Health")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    MWS_Boundaries_asset_id = ti.xcom_pull(task_ids='MWS_Boundaries', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Soil_Health')
    version = algo_config['version']
    execution_modes = algo_config['execution_modes']
    
    print(f"📌 Active Version: {version}")
    print(f"📋 Execution Modes: {execution_modes}")
    
    # Get API and Docker configs
    api_config = execution_modes.get('api', {})
    docker_config = execution_modes.get('docker', {})
    
    # Determine execution mode based on priority
    api_enabled = api_config.get('enabled', False)
    docker_enabled = docker_config.get('enabled', False)
    
    api_priority = api_config.get('priority', 99) if api_enabled else 99
    docker_priority = docker_config.get('priority', 99) if docker_enabled else 99
    
    use_docker = docker_enabled and (docker_priority < api_priority)
    use_api = api_enabled and not use_docker
    
    # Build algorithm parameters (for both API and Docker)
    algo_params = {
        'execution_id': run_id,
        'state': state,
        'district': district,
        'block': block,
        'MWS_Boundaries': MWS_Boundaries_asset_id
    }
    
    print(f"🔍 Parameters: {algo_params}")
    
    if use_docker:
        # ===== DOCKER EXECUTION =====
        print(f"🐳 Docker Mode Selected (priority: {docker_priority})")
        print(f"   Image: {docker_config.get('image')}")
        print(f"   Module: {docker_config.get('module')}")
        print(f"   Function: {docker_config.get('function')}")
        
        # Import Docker runner
        AIRFLOW_HOME_RT = os.getenv('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
        sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd'))
        from simple_docker_runner import run_in_docker
        
        # Extract function params (remove execution_id for function call)
        function_params = {k: v for k, v in algo_params.items() if k != 'execution_id'}



        # Execute in Docker
        docker_result = run_in_docker(
            image=docker_config['image'],
            module_path=docker_config['module'],
            function_name=docker_config['function'],
            function_params=function_params
        )

        # Parse structured result
        asset_ids = docker_result.get('asset_ids', [])
        hosting_platform = docker_result.get('hosting_platform', 'GEE')
        stac_spec = docker_result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else ['unknown']
        
        stac_raw = docker_result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

        
    elif use_api:
        # ===== API EXECUTION =====
        print(f" API Mode Selected (priority: {api_priority})")
        print(f"   URL: {api_config['url']}")

        try:
            from airflow.models import Variable
            token = Variable.get("CORESTACK_AUTH_TOKEN", default_var=None)
        except Exception as token_err:
            print(f"WARNING: Could not fetch CORESTACK_AUTH_TOKEN: {token_err}")
            token = None

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.post(
                api_config['url'],
                json=algo_params,
                headers=headers,
                timeout=7200  # 2 hours — GEE tasks take long
            )
        except requests.exceptions.Timeout:
            raise Exception(
                "GENERATION_ERROR: API request timed out after 2 hours"
            )
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(
                f"GENERATION_ERROR: Cannot connect to CoreStack API — {conn_err}"
            )

        if response.status_code == 400:
            error_msg = response.text
            print(f"[INVALID_INVOCATION] HTTP 400 from API: {error_msg}")
            raise AirflowSkipException(
                f"INVALID_INVOCATION: Bad input params — {error_msg}"
            )
        elif response.status_code == 404:
            error_msg = response.text
            print(f"[NO_DATA] HTTP 404 from API: {error_msg}")
            raise AirflowSkipException(
                f"NO_DATA: No data for this location/params — {error_msg}"
            )
        elif response.status_code >= 500:
            error_msg = response.text
            print(f"[GENERATION_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"GENERATION_ERROR: Pipeline failed (HTTP {response.status_code}) — {error_msg}"
            )
        elif not response.ok:
            error_msg = response.text
            print(f"[UNEXPECTED_ERROR] HTTP {response.status_code} from API: {error_msg}")
            raise Exception(
                f"Unexpected HTTP {response.status_code}: {error_msg}"
            )
        result = response.json()

        # Parse structured result
        asset_ids = result.get('asset_ids', [])
        hosting_platform = result.get('hosting_platform', 'GEE')
        stac_spec = result.get('stac_spec', None)
        asset_id = asset_ids if asset_ids else [result.get('asset_id', 'unknown')]
        
        # STACD-IMPL: extract stac_items from stac_spec or items key
        stac_raw = result.get('stac_items', None)
        if isinstance(stac_raw, list):
            stac_items = stac_raw
        elif isinstance(stac_raw, dict):
            stac_items = [stac_raw]
        else:
            stac_items = None

    
    else:
        raise ValueError(f"No execution mode enabled for Soil_Health")
    
    print(f"✓ Soil_Health completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Soil_Health',
            version=version,
            run_id=run_id,
            execution_params=algo_params,
            output_ref=asset_id,
            status='success'
        )
    finally:
        db.close()
    
    # Push to XCom for downstream
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='hosting_platform', value=hosting_platform)
    ti.xcom_push(key='stac_spec', value=stac_spec)
    
    # STACD-IMPL: push stac_items for downstream dataset registration
    ti.xcom_push(key='stac_items', value=stac_items)
    # Push job_id if present in API response (bioacoustic pipelines)
    ti.xcom_push(key='job_id', value=result.get('job_id') if 'result' in dir() else None)
    
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'hosting_platform': hosting_platform, 'stac_items': stac_items}


# ========== Dataset Registration Functions ==========

def register_Change_Detection_Asset(**context):
    """Register Change_Detection_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Change_Detection_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Change_Detection_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Change_Detection_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Change_Detection_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Change_Detection_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Change_Detection_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Change_Detection_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Change_Detection_Vector_Asset(**context):
    """Register Change_Detection_Vector_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Change_Detection_Vector_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Change_Detection_Vector_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Change_Detection_Vector_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Change_Detection_Vector_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Change_Detection_Vector_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Change_Detection_Vector_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Change_Detection_Vector_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Cropping_Intensity_Asset(**context):
    """Register Cropping_Intensity_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Cropping_Intensity_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Cropping_Intensity_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Cropping_Intensity_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Cropping_Intensity_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Cropping_Intensity_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Cropping_Intensity_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Cropping_Intensity_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_LULC_Raster(**context):
    """Register LULC_Raster dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: LULC_Raster")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering LULC_Raster")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="LULC_Raster",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"LULC_Raster registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'LULC_Raster',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'LULC_Raster_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "LULC_Raster", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_LULC_Terrain_Plain_Asset(**context):
    """Register LULC_Terrain_Plain_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: LULC_Terrain_Plain_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering LULC_Terrain_Plain_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="LULC_Terrain_Plain_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"LULC_Terrain_Plain_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'LULC_Terrain_Plain_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'LULC_Terrain_Plain_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "LULC_Terrain_Plain_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_LULC_Terrain_Slope_Asset(**context):
    """Register LULC_Terrain_Slope_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: LULC_Terrain_Slope_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering LULC_Terrain_Slope_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="LULC_Terrain_Slope_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"LULC_Terrain_Slope_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'LULC_Terrain_Slope_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'LULC_Terrain_Slope_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "LULC_Terrain_Slope_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_LULC_Vector(**context):
    """Register LULC_Vector dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: LULC_Vector")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering LULC_Vector")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="LULC_Vector",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"LULC_Vector registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'LULC_Vector',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'LULC_Vector_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "LULC_Vector", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def fetch_MWS_Boundaries(**context):
    """
    Fetch MWS_Boundaries root dataset from database
    Root datasets are pre-existing and not produced by algorithms.
    """
    print("="*60)
    print("Fetching Root Dataset: MWS_Boundaries")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    
    # Extract region parameters
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    execution_type = params.get('execution_type', 'fullexec')
    updated_dataset = params.get('updated_dataset', '')
    is_being_updated = (execution_type == 'update_dataset' and updated_dataset == 'MWS_Boundaries')

    print(f"🔍 Looking for MWS_Boundaries in region: {state}/{district}/{block}")
    if is_being_updated:
        print(f"⚡ update_dataset mode — will use latest registered version")

    # Query database for root dataset (always gets latest version)
    db = STACDDatabase(DB_PATH)
    try:
        root_dataset = db.get_root_dataset(
            dataset_type_id='MWS_Boundaries',
            state=state,
            district=district,
            block=block
        )
        
        if not root_dataset:
            raise ValueError(f"Root dataset MWS_Boundaries not found for {state}/{district}/{block}")
        
        asset_id = root_dataset.asset_id
        version = root_dataset.version
        print(f"✓ Found MWS_Boundaries version {version}")
        print(f"   Asset ID: {asset_id}")
        if is_being_updated:
            print(f"⚡ Propagating updated dataset v{version} to downstream algorithms")
        
    finally:
        db.close()
    
    # Push asset_id for downstream consumption
    ti.xcom_push(key='asset_id', value=asset_id)
    ti.xcom_push(key='version', value=version)
    ti.xcom_push(key='is_updated', value=is_being_updated)
    return {'status': 'success', 'asset_id': asset_id, 'version': version, 'is_root': True, 'is_updated': is_being_updated}


def register_NREGA_Layer(**context):
    """Register NREGA_Layer dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: NREGA_Layer")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering NREGA_Layer")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="NREGA_Layer",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"NREGA_Layer registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'NREGA_Layer',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'NREGA_Layer_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "NREGA_Layer", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_SWB_Layer_Asset(**context):
    """Register SWB_Layer_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: SWB_Layer_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering SWB_Layer_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="SWB_Layer_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"SWB_Layer_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'SWB_Layer_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'SWB_Layer_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "SWB_Layer_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Soil_Health_Asset(**context):
    """Register Soil_Health_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Soil_Health_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Soil_Health_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Soil_Health_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Soil_Health_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Soil_Health_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Soil_Health_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Soil_Health_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Terrain_Raster(**context):
    """Register Terrain_Raster dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Terrain_Raster")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Terrain_Raster")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Terrain_Raster",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Terrain_Raster registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Terrain_Raster',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Terrain_Raster_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Terrain_Raster", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Terrain_Vector(**context):
    """Register Terrain_Vector dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Terrain_Vector")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Terrain_Vector")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Terrain_Vector",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Terrain_Vector registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Terrain_Vector',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Terrain_Vector_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Terrain_Vector", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Tree_Health_CCD_Raster_Asset(**context):
    """Register Tree_Health_CCD_Raster_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Tree_Health_CCD_Raster_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Tree_Health_CCD_Raster_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Tree_Health_CCD_Raster_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Tree_Health_CCD_Raster_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Tree_Health_CCD_Raster_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Tree_Health_CCD_Raster_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Tree_Health_CCD_Raster_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Tree_Health_CCD_Vector_Asset(**context):
    """Register Tree_Health_CCD_Vector_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Tree_Health_CCD_Vector_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Tree_Health_CCD_Vector_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Tree_Health_CCD_Vector_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Tree_Health_CCD_Vector_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Tree_Health_CCD_Vector_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Tree_Health_CCD_Vector_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Tree_Health_CCD_Vector_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Tree_Health_CH_Raster_Asset(**context):
    """Register Tree_Health_CH_Raster_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Tree_Health_CH_Raster_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Tree_Health_CH_Raster_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Tree_Health_CH_Raster_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Tree_Health_CH_Raster_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Tree_Health_CH_Raster_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Tree_Health_CH_Raster_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Tree_Health_CH_Raster_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Tree_Health_CH_Vector_Asset(**context):
    """Register Tree_Health_CH_Vector_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Tree_Health_CH_Vector_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Tree_Health_CH_Vector_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Tree_Health_CH_Vector_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Tree_Health_CH_Vector_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Tree_Health_CH_Vector_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Tree_Health_CH_Vector_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Tree_Health_CH_Vector_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Tree_Health_OC_Raster_Asset(**context):
    """Register Tree_Health_OC_Raster_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Tree_Health_OC_Raster_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Tree_Health_OC_Raster_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Tree_Health_OC_Raster_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Tree_Health_OC_Raster_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Tree_Health_OC_Raster_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Tree_Health_OC_Raster_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Tree_Health_OC_Raster_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Tree_Health_OC_Vector_Asset(**context):
    """Register Tree_Health_OC_Vector_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Tree_Health_OC_Vector_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering Tree_Health_OC_Vector_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="Tree_Health_OC_Vector_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Tree_Health_OC_Vector_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'Tree_Health_OC_Vector_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Tree_Health_OC_Vector_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "Tree_Health_OC_Vector_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_ZOI_Asset(**context):
    """Register ZOI_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: ZOI_Asset")
    print("=" * 60)
    
    ti = context["ti"]
    run_id = context["run_id"]
    params = context["params"]
    
    # Find upstream algorithm
    upstream_tasks = context["task"].upstream_task_ids
    asset_id = None
    producing_algo = None
    algo_version = None
    stac_items = None


    for upstream_task_id in upstream_tasks:
        xcom_asset = ti.xcom_pull(task_ids=upstream_task_id, key="asset_id")
        if xcom_asset and xcom_asset != 'unknown' and xcom_asset != ['unknown']:
            asset_id = xcom_asset
            if isinstance(asset_id, str):
                asset_id = [asset_id]
            producing_algo = upstream_task_id
            algo_version = ti.xcom_pull(task_ids=upstream_task_id, key="version")
            hosting_platform = ti.xcom_pull(task_ids=upstream_task_id, key="hosting_platform") or "GEE"
            stac_spec = ti.xcom_pull(task_ids=upstream_task_id, key="stac_spec")
            stac_items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            break

    if not asset_id:
        raise AirflowSkipException(
            f"Skipping dataset registration — upstream task was "
            f"skipped (INVALID_INVOCATION or NO_DATA)"
        )

    print(f"Registering ZOI_Asset")
    print(f"Asset IDs: {asset_id}")
    print(f"Produced by: {producing_algo} v{algo_version}")

    # Pull job_id pushed by execute task (bioacoustic only)
    job_id_val = None
    for upstream_task_id in upstream_tasks:
        job_id_val = ti.xcom_pull(task_ids=upstream_task_id, key="job_id")
        if job_id_val:
            break

    # Build group-aware meta_info
    if GROUP == 'bioacoustic':
        project_id_val = params.get('project', params.get('project_id', 'unknown'))
        # Extract algo name from stac_items API response (cem:algorithm minus .py)
        algo_val = 'unknown'
        for upstream_task_id in upstream_tasks:
            _items = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if _items and isinstance(_items, list) and _items:
                _cem_algo = _items[0].get('properties', {}).get('cem:algorithm', '')
                if _cem_algo:
                    algo_val = _cem_algo.replace('.py', '')
                    break
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'bioacoustic',
            'project_id': project_id_val,
            'algo': algo_val,
            'job_id': job_id_val,
        }
    elif GROUP == 'drone':
        project_id_val = params.get('project_id', 'unknown')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'drone',
            'project_id': project_id_val,
        }
    elif GROUP == 'custom_lulc':
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'custom_lulc',
        }
    else:  # corestack default
        state = params.get('state')
        district = params.get('district')
        block = params.get('block')
        start_year = params.get('start_year')
        end_year = params.get('end_year')
        meta_info_val = {
            'registered_at': str(datetime.now()),
            'group': 'corestack',
            'state': state,
            'district': district,
            'block': block,
            'start_year': str(start_year),
            'end_year': str(end_year),
        }

    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        instances = []
        for single_asset_id in asset_id:
            inst = db.log_dataset_instance(
                dataset_type_id="ZOI_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"ZOI_Asset registered in database")
        print(f"Version: {version}")

        # STACD-IMPL: pull stac_items from upstream algo XCom
        stac_items_raw = []
        for upstream_task_id in upstream_tasks:
            pulled = ti.xcom_pull(task_ids=upstream_task_id, key="stac_items")
            if pulled:
                stac_items_raw = pulled
                break

        enriched_success = False
        if stac_items_raw:
            try:
                # STACD-IMPL: enrichment path — flat list of STAC Feature dicts from API
                import sys, os, json
                sys.path.insert(0, os.path.join(AIRFLOW_HOME_RT, 'stacd/stac_export'))
                from config import CATALOG_OUTPUT_DIR, CATALOG_BASE_URL, STACD_BROWSER_URL

                # Group-aware output directory
                if GROUP == 'bioacoustic':
                    _proj = params.get('project', params.get('project_id', 'unknown'))
                    _algo = algo_val  # extracted from cem:algorithm above
                    _job  = job_id_val or 'unknown_job'
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'bioacoustic' / _proj / _algo / _job
                elif GROUP == 'drone':
                    _proj = params.get('project_id', 'unknown')
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'drone' / _proj
                elif GROUP == 'custom_lulc':
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'custom_lulc' / 'unknown' / 'unknown' / 'unknown'
                else:
                    out_dir = CATALOG_OUTPUT_DIR / 'datasets' / 'corestack' / params.get('state', 'unknown') / params.get('district', 'unknown') / params.get('block', 'unknown')
                out_dir.mkdir(parents=True, exist_ok=True)

                for item in stac_items_raw:
                    if not isinstance(item, dict) or item.get('type') != 'Feature':
                        continue

                    # Append STACD provenance
                    item.setdefault('properties', {}).update({
                        'stacd:dag_id': DAG_ID,
                        'stacd:run_id': run_id,
                        'stacd:algo_type_id': producing_algo,
                        'stacd:algo_version': algo_version,
                        'stacd:dataset_type_id': 'ZOI_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'ZOI_Asset_{instance.instance_id}')
                    action_suffix = f"_{params.get('action')}" if params.get('action') else ''
                    out_filename = f'{item_id}_v{version}{action_suffix}.json'
                    out_path = out_dir / out_filename

                    # STACD-IMPL: fix self/root links to point at actual served catalog location,
                    # regardless of what (if anything) the upstream backend returned for these.
                    try:
                        rel_path = out_path.relative_to(CATALOG_OUTPUT_DIR)
                        self_href = CATALOG_BASE_URL.rstrip('/') + '/' + str(rel_path).replace(os.sep, '/')
                        root_href = CATALOG_BASE_URL.rstrip('/') + '/catalog.json'
                        fixed_links = [l for l in item.get('links', []) if l.get('rel') not in ('self', 'root')]
                        fixed_links.append({'rel': 'root', 'href': root_href, 'type': 'application/json'})
                        fixed_links.append({'rel': 'self', 'href': self_href, 'type': 'application/json'})
                        item['links'] = fixed_links
                    except Exception as link_err:
                        print(f'STACD-IMPL: could not fix self/root links: {link_err}')

                    with open(out_path, 'w') as f:
                        json.dump(item, f, indent=2, ensure_ascii=False)
                    print(f'STAC item written: {out_path}')
                enriched_success = True
            except Exception as e:
                print(f"STACD-IMPL Error during enrichment: {e}")
                enriched_success = False

        if not enriched_success:
            if stac_spec:
                augment_and_write_stac(stac_spec, "ZOI_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

# ========== DAG Definition ==========

default_args = {
    'owner': 'stacd',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Local-compute pipeline for dynamic layers (yearly/derived layers with inter-dependencies) from local_layer_map.json.",
    schedule_interval=None,
    catchup=False,
    tags=['stacd', 'recompute', 'generic'],
    params={
        'state': 'default_value',
        'district': 'default_value',
        'block': 'default_value',
        'start_year': 'default_value',
        'end_year': 'default_value',
        'gee_account_id': 'default_value',
        'execution_type': 'fullexec',
        'updated_algo': '',
        'updated_dataset': ''
    },
    access_control={"CoreStack_Op": {"DAGs": {"can_read", "can_edit", "can_delete"}, "DAG Runs": {"can_read", "can_create", "can_delete", "menu_access"}}},
    
) as dag:
    
    # Branch task to determine execution path
    branch_task = BranchPythonOperator(
        task_id='determine_execution_path',
        python_callable=determine_execution_path,
        provide_context=True
    )
    
    # Create all algorithm tasks

    NREGA_Clip = PythonOperator(
        task_id='NREGA_Clip',
        python_callable=execute_NREGA_Clip,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    LULC_Algorithm = PythonOperator(
        task_id='LULC_Algorithm',
        python_callable=execute_LULC_Algorithm,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    LULC_Vectorization = PythonOperator(
        task_id='LULC_Vectorization',
        python_callable=execute_LULC_Vectorization,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Change_Detection = PythonOperator(
        task_id='Change_Detection',
        python_callable=execute_Change_Detection,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Change_Detection_Vector = PythonOperator(
        task_id='Change_Detection_Vector',
        python_callable=execute_Change_Detection_Vector,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Terrain_Algorithm = PythonOperator(
        task_id='Terrain_Algorithm',
        python_callable=execute_Terrain_Algorithm,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Terrain_Clusters = PythonOperator(
        task_id='Terrain_Clusters',
        python_callable=execute_Terrain_Clusters,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    LULC_Terrain_Plain = PythonOperator(
        task_id='LULC_Terrain_Plain',
        python_callable=execute_LULC_Terrain_Plain,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    LULC_Terrain_Slope = PythonOperator(
        task_id='LULC_Terrain_Slope',
        python_callable=execute_LULC_Terrain_Slope,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Cropping_Intensity = PythonOperator(
        task_id='Cropping_Intensity',
        python_callable=execute_Cropping_Intensity,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    SWB_Layer = PythonOperator(
        task_id='SWB_Layer',
        python_callable=execute_SWB_Layer,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    ZOI = PythonOperator(
        task_id='ZOI',
        python_callable=execute_ZOI,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Tree_Health_CH_Raster = PythonOperator(
        task_id='Tree_Health_CH_Raster',
        python_callable=execute_Tree_Health_CH_Raster,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Tree_Health_CH_Vector = PythonOperator(
        task_id='Tree_Health_CH_Vector',
        python_callable=execute_Tree_Health_CH_Vector,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Tree_Health_CCD_Raster = PythonOperator(
        task_id='Tree_Health_CCD_Raster',
        python_callable=execute_Tree_Health_CCD_Raster,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Tree_Health_CCD_Vector = PythonOperator(
        task_id='Tree_Health_CCD_Vector',
        python_callable=execute_Tree_Health_CCD_Vector,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Tree_Health_OC_Raster = PythonOperator(
        task_id='Tree_Health_OC_Raster',
        python_callable=execute_Tree_Health_OC_Raster,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Tree_Health_OC_Vector = PythonOperator(
        task_id='Tree_Health_OC_Vector',
        python_callable=execute_Tree_Health_OC_Vector,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Soil_Health = PythonOperator(
        task_id='Soil_Health',
        python_callable=execute_Soil_Health,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Change_Detection_Asset = PythonOperator(
        task_id='Change_Detection_Asset',
        python_callable=register_Change_Detection_Asset,
        provide_context=True
    )

    Change_Detection_Vector_Asset = PythonOperator(
        task_id='Change_Detection_Vector_Asset',
        python_callable=register_Change_Detection_Vector_Asset,
        provide_context=True
    )

    Cropping_Intensity_Asset = PythonOperator(
        task_id='Cropping_Intensity_Asset',
        python_callable=register_Cropping_Intensity_Asset,
        provide_context=True
    )

    LULC_Raster = PythonOperator(
        task_id='LULC_Raster',
        python_callable=register_LULC_Raster,
        provide_context=True
    )

    LULC_Terrain_Plain_Asset = PythonOperator(
        task_id='LULC_Terrain_Plain_Asset',
        python_callable=register_LULC_Terrain_Plain_Asset,
        provide_context=True
    )

    LULC_Terrain_Slope_Asset = PythonOperator(
        task_id='LULC_Terrain_Slope_Asset',
        python_callable=register_LULC_Terrain_Slope_Asset,
        provide_context=True
    )

    LULC_Vector = PythonOperator(
        task_id='LULC_Vector',
        python_callable=register_LULC_Vector,
        provide_context=True
    )

    MWS_Boundaries = PythonOperator(
        task_id='MWS_Boundaries',
        python_callable=fetch_MWS_Boundaries,
        provide_context=True
    )

    NREGA_Layer = PythonOperator(
        task_id='NREGA_Layer',
        python_callable=register_NREGA_Layer,
        provide_context=True
    )

    SWB_Layer_Asset = PythonOperator(
        task_id='SWB_Layer_Asset',
        python_callable=register_SWB_Layer_Asset,
        provide_context=True
    )

    Soil_Health_Asset = PythonOperator(
        task_id='Soil_Health_Asset',
        python_callable=register_Soil_Health_Asset,
        provide_context=True
    )

    Terrain_Raster = PythonOperator(
        task_id='Terrain_Raster',
        python_callable=register_Terrain_Raster,
        provide_context=True
    )

    Terrain_Vector = PythonOperator(
        task_id='Terrain_Vector',
        python_callable=register_Terrain_Vector,
        provide_context=True
    )

    Tree_Health_CCD_Raster_Asset = PythonOperator(
        task_id='Tree_Health_CCD_Raster_Asset',
        python_callable=register_Tree_Health_CCD_Raster_Asset,
        provide_context=True
    )

    Tree_Health_CCD_Vector_Asset = PythonOperator(
        task_id='Tree_Health_CCD_Vector_Asset',
        python_callable=register_Tree_Health_CCD_Vector_Asset,
        provide_context=True
    )

    Tree_Health_CH_Raster_Asset = PythonOperator(
        task_id='Tree_Health_CH_Raster_Asset',
        python_callable=register_Tree_Health_CH_Raster_Asset,
        provide_context=True
    )

    Tree_Health_CH_Vector_Asset = PythonOperator(
        task_id='Tree_Health_CH_Vector_Asset',
        python_callable=register_Tree_Health_CH_Vector_Asset,
        provide_context=True
    )

    Tree_Health_OC_Raster_Asset = PythonOperator(
        task_id='Tree_Health_OC_Raster_Asset',
        python_callable=register_Tree_Health_OC_Raster_Asset,
        provide_context=True
    )

    Tree_Health_OC_Vector_Asset = PythonOperator(
        task_id='Tree_Health_OC_Vector_Asset',
        python_callable=register_Tree_Health_OC_Vector_Asset,
        provide_context=True
    )

    ZOI_Asset = PythonOperator(
        task_id='ZOI_Asset',
        python_callable=register_ZOI_Asset,
        provide_context=True
    )

    # ============================================================================
    # DEPENDENCIES (Generated from YAML input_datasets and outputs)
    # ============================================================================

    # Branch connects to all algorithms (selective execution)
    branch_task >> NREGA_Clip
    branch_task >> LULC_Algorithm
    branch_task >> LULC_Vectorization
    branch_task >> Change_Detection
    branch_task >> Change_Detection_Vector
    branch_task >> Terrain_Algorithm
    branch_task >> Terrain_Clusters
    branch_task >> LULC_Terrain_Plain
    branch_task >> LULC_Terrain_Slope
    branch_task >> Cropping_Intensity
    branch_task >> SWB_Layer
    branch_task >> ZOI
    branch_task >> Tree_Health_CH_Raster
    branch_task >> Tree_Health_CH_Vector
    branch_task >> Tree_Health_CCD_Raster
    branch_task >> Tree_Health_CCD_Vector
    branch_task >> Tree_Health_OC_Raster
    branch_task >> Tree_Health_OC_Vector
    branch_task >> Soil_Health

 # Branch connects to all root datasets (for update_dataset support)
    branch_task >> MWS_Boundaries

    # Algorithm -> Dataset dependencies (outputs)
    NREGA_Clip >> NREGA_Layer
    LULC_Algorithm >> LULC_Raster
    LULC_Vectorization >> LULC_Vector
    Change_Detection >> Change_Detection_Asset
    Change_Detection_Vector >> Change_Detection_Vector_Asset
    Terrain_Algorithm >> Terrain_Raster
    Terrain_Clusters >> Terrain_Vector
    LULC_Terrain_Plain >> LULC_Terrain_Plain_Asset
    LULC_Terrain_Slope >> LULC_Terrain_Slope_Asset
    Cropping_Intensity >> Cropping_Intensity_Asset
    SWB_Layer >> SWB_Layer_Asset
    ZOI >> ZOI_Asset
    Tree_Health_CH_Raster >> Tree_Health_CH_Raster_Asset
    Tree_Health_CH_Vector >> Tree_Health_CH_Vector_Asset
    Tree_Health_CCD_Raster >> Tree_Health_CCD_Raster_Asset
    Tree_Health_CCD_Vector >> Tree_Health_CCD_Vector_Asset
    Tree_Health_OC_Raster >> Tree_Health_OC_Raster_Asset
    Tree_Health_OC_Vector >> Tree_Health_OC_Vector_Asset
    Soil_Health >> Soil_Health_Asset

    # Dataset -> Algorithm dependencies (inputs)
    MWS_Boundaries >> NREGA_Clip
    MWS_Boundaries >> LULC_Algorithm
    LULC_Raster >> LULC_Vectorization
    MWS_Boundaries >> Change_Detection
    Change_Detection_Asset >> Change_Detection_Vector
    MWS_Boundaries >> Terrain_Algorithm
    Terrain_Raster >> Terrain_Clusters
    Terrain_Raster >> LULC_Terrain_Plain
    LULC_Raster >> LULC_Terrain_Plain
    Terrain_Raster >> LULC_Terrain_Slope
    LULC_Raster >> LULC_Terrain_Slope
    MWS_Boundaries >> Cropping_Intensity
    MWS_Boundaries >> SWB_Layer
    SWB_Layer_Asset >> ZOI
    LULC_Raster >> Tree_Health_CH_Raster
    Tree_Health_CH_Raster_Asset >> Tree_Health_CH_Vector
    LULC_Raster >> Tree_Health_CCD_Raster
    Tree_Health_CCD_Raster_Asset >> Tree_Health_CCD_Vector
    Change_Detection_Asset >> Tree_Health_OC_Raster
    Tree_Health_OC_Raster_Asset >> Tree_Health_OC_Vector
    MWS_Boundaries >> Soil_Health
