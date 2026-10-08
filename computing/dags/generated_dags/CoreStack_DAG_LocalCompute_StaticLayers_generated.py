"""
Auto-generated STACD DAG: STACD Local Compute DAG - Static Layers (v1)
Generated at: 2026-09-10T08:39:07.694645
Description: Local-compute pipeline for static layers (independent, dependency-free layers) from local_layer_map.json.

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
DAG_ID = "CoreStack_DAG_LocalCompute_StaticLayers"
GROUP = "corestack"
DB_PATH = os.path.join(AIRFLOW_HOME_RT, 'stacd/database/stacd_database.db')

# ========== Dependency Graph (for reference) ==========
dependencies = {
    "Aquifer_Vector": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Aquifer_Vector_Asset"
        ]
    },
    "Livestock": {
        "inputs": [
            "Admin_Boundary_Asset"
        ],
        "outputs": [
            "Livestock_Asset"
        ]
    },
    "Antyodaya": {
        "inputs": [
            "Admin_Boundary_Asset"
        ],
        "outputs": [
            "Antyodaya_Asset"
        ]
    },
    "Drainage_Density": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Drainage_Density_Asset"
        ]
    },
    "River": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "River_Asset"
        ]
    },
    "Canal": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Canal_Asset"
        ]
    },
    "Digital_Elevation_Model": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Digital_Elevation_Model_Asset"
        ]
    },
    "Facilities_Proximity": {
        "inputs": [
            "Admin_Boundary_Asset"
        ],
        "outputs": [
            "Facilities_Proximity_Asset"
        ]
    },
    "Drainage_Lines": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Drainage_Lines_Asset"
        ]
    },
    "Restoration_Opportunity": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Restoration_Opportunity_Asset"
        ]
    },
    "SOGE_Vector": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "SOGE_Vector_Asset"
        ]
    },
    "LCW_Conflict": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "LCW_Conflict_Asset"
        ]
    },
    "Agro_Ecological": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Agro_Ecological_Asset"
        ]
    },
    "Factory_CSR": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Factory_CSR_Asset"
        ]
    },
    "Green_Credit": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Green_Credit_Asset"
        ]
    },
    "Mining": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Mining_Asset"
        ]
    },
    "Natural_Depression": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Natural_Depression_Asset"
        ]
    },
    "Dist_to_Drainage": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Dist_to_Drainage_Asset"
        ]
    },
    "Catchment_Area": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Catchment_Area_Asset"
        ]
    },
    "Slope_Percentage": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Slope_Percentage_Asset"
        ]
    },
    "MWS_Connectivity": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "MWS_Connectivity_Asset"
        ]
    },
    "MWS_Centroid": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "MWS_Centroid_Asset"
        ]
    },
    "Soil_Type": {
        "inputs": [
            "MWS_Boundaries"
        ],
        "outputs": [
            "Soil_Type_Asset"
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
        root_datasets = ['Admin_Boundary_Asset', 'MWS_Boundaries']
        root_algos = ['Aquifer_Vector', 'Livestock', 'Antyodaya', 'Drainage_Density', 'River', 'Canal', 'Digital_Elevation_Model', 'Facilities_Proximity', 'Drainage_Lines', 'Restoration_Opportunity', 'SOGE_Vector', 'LCW_Conflict', 'Agro_Ecological', 'Factory_CSR', 'Green_Credit', 'Mining', 'Natural_Depression', 'Dist_to_Drainage', 'Catchment_Area', 'Slope_Percentage', 'MWS_Connectivity', 'MWS_Centroid', 'Soil_Type']
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

def execute_Aquifer_Vector(**context):
    """
    Execute Aquifer_Vector - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Aquifer_Vector")
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
    algo_config = get_active_algorithm_config('Aquifer_Vector')
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
        raise ValueError(f"No execution mode enabled for Aquifer_Vector")
    
    print(f"✓ Aquifer_Vector completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Aquifer_Vector',
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


def execute_Livestock(**context):
    """
    Execute Livestock - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Livestock")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Admin_Boundary_Asset_asset_id = ti.xcom_pull(task_ids='Admin_Boundary_Asset', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Livestock')
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
        'Admin_Boundary_Asset': Admin_Boundary_Asset_asset_id
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
        raise ValueError(f"No execution mode enabled for Livestock")
    
    print(f"✓ Livestock completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Livestock',
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


def execute_Antyodaya(**context):
    """
    Execute Antyodaya - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Antyodaya")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Admin_Boundary_Asset_asset_id = ti.xcom_pull(task_ids='Admin_Boundary_Asset', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Antyodaya')
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
        'Admin_Boundary_Asset': Admin_Boundary_Asset_asset_id
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
        raise ValueError(f"No execution mode enabled for Antyodaya")
    
    print(f"✓ Antyodaya completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Antyodaya',
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


def execute_Drainage_Density(**context):
    """
    Execute Drainage_Density - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Drainage_Density")
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
    algo_config = get_active_algorithm_config('Drainage_Density')
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
        raise ValueError(f"No execution mode enabled for Drainage_Density")
    
    print(f"✓ Drainage_Density completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Drainage_Density',
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


def execute_River(**context):
    """
    Execute River - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: River")
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
    algo_config = get_active_algorithm_config('River')
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
        raise ValueError(f"No execution mode enabled for River")
    
    print(f"✓ River completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='River',
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


def execute_Canal(**context):
    """
    Execute Canal - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Canal")
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
    algo_config = get_active_algorithm_config('Canal')
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
        raise ValueError(f"No execution mode enabled for Canal")
    
    print(f"✓ Canal completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Canal',
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


def execute_Digital_Elevation_Model(**context):
    """
    Execute Digital_Elevation_Model - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Digital_Elevation_Model")
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
    algo_config = get_active_algorithm_config('Digital_Elevation_Model')
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
        raise ValueError(f"No execution mode enabled for Digital_Elevation_Model")
    
    print(f"✓ Digital_Elevation_Model completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Digital_Elevation_Model',
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


def execute_Facilities_Proximity(**context):
    """
    Execute Facilities_Proximity - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Facilities_Proximity")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    run_id = context['run_id']

    # Get inputs from upstream dataset tasks
    Admin_Boundary_Asset_asset_id = ti.xcom_pull(task_ids='Admin_Boundary_Asset', key='asset_id')

    # Extract parameters from context
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    
    # Query database for active version
    algo_config = get_active_algorithm_config('Facilities_Proximity')
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
        'Admin_Boundary_Asset': Admin_Boundary_Asset_asset_id
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
        raise ValueError(f"No execution mode enabled for Facilities_Proximity")
    
    print(f"✓ Facilities_Proximity completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Facilities_Proximity',
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


def execute_Drainage_Lines(**context):
    """
    Execute Drainage_Lines - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Drainage_Lines")
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
    algo_config = get_active_algorithm_config('Drainage_Lines')
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
        raise ValueError(f"No execution mode enabled for Drainage_Lines")
    
    print(f"✓ Drainage_Lines completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Drainage_Lines',
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


def execute_Restoration_Opportunity(**context):
    """
    Execute Restoration_Opportunity - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Restoration_Opportunity")
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
    algo_config = get_active_algorithm_config('Restoration_Opportunity')
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
        raise ValueError(f"No execution mode enabled for Restoration_Opportunity")
    
    print(f"✓ Restoration_Opportunity completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Restoration_Opportunity',
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


def execute_SOGE_Vector(**context):
    """
    Execute SOGE_Vector - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: SOGE_Vector")
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
    algo_config = get_active_algorithm_config('SOGE_Vector')
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
        raise ValueError(f"No execution mode enabled for SOGE_Vector")
    
    print(f"✓ SOGE_Vector completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='SOGE_Vector',
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


def execute_LCW_Conflict(**context):
    """
    Execute LCW_Conflict - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: LCW_Conflict")
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
    algo_config = get_active_algorithm_config('LCW_Conflict')
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
        raise ValueError(f"No execution mode enabled for LCW_Conflict")
    
    print(f"✓ LCW_Conflict completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='LCW_Conflict',
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


def execute_Agro_Ecological(**context):
    """
    Execute Agro_Ecological - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Agro_Ecological")
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
    algo_config = get_active_algorithm_config('Agro_Ecological')
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
        raise ValueError(f"No execution mode enabled for Agro_Ecological")
    
    print(f"✓ Agro_Ecological completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Agro_Ecological',
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


def execute_Factory_CSR(**context):
    """
    Execute Factory_CSR - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Factory_CSR")
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
    algo_config = get_active_algorithm_config('Factory_CSR')
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
        raise ValueError(f"No execution mode enabled for Factory_CSR")
    
    print(f"✓ Factory_CSR completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Factory_CSR',
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


def execute_Green_Credit(**context):
    """
    Execute Green_Credit - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Green_Credit")
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
    algo_config = get_active_algorithm_config('Green_Credit')
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
        raise ValueError(f"No execution mode enabled for Green_Credit")
    
    print(f"✓ Green_Credit completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Green_Credit',
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


def execute_Mining(**context):
    """
    Execute Mining - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Mining")
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
    algo_config = get_active_algorithm_config('Mining')
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
        raise ValueError(f"No execution mode enabled for Mining")
    
    print(f"✓ Mining completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Mining',
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


def execute_Natural_Depression(**context):
    """
    Execute Natural_Depression - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Natural_Depression")
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
    algo_config = get_active_algorithm_config('Natural_Depression')
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
        raise ValueError(f"No execution mode enabled for Natural_Depression")
    
    print(f"✓ Natural_Depression completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Natural_Depression',
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


def execute_Dist_to_Drainage(**context):
    """
    Execute Dist_to_Drainage - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Dist_to_Drainage")
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
    algo_config = get_active_algorithm_config('Dist_to_Drainage')
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
        raise ValueError(f"No execution mode enabled for Dist_to_Drainage")
    
    print(f"✓ Dist_to_Drainage completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Dist_to_Drainage',
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


def execute_Catchment_Area(**context):
    """
    Execute Catchment_Area - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Catchment_Area")
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
    algo_config = get_active_algorithm_config('Catchment_Area')
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
        raise ValueError(f"No execution mode enabled for Catchment_Area")
    
    print(f"✓ Catchment_Area completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Catchment_Area',
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


def execute_Slope_Percentage(**context):
    """
    Execute Slope_Percentage - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Slope_Percentage")
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
    algo_config = get_active_algorithm_config('Slope_Percentage')
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
        raise ValueError(f"No execution mode enabled for Slope_Percentage")
    
    print(f"✓ Slope_Percentage completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Slope_Percentage',
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


def execute_MWS_Connectivity(**context):
    """
    Execute MWS_Connectivity - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: MWS_Connectivity")
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
    algo_config = get_active_algorithm_config('MWS_Connectivity')
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
        raise ValueError(f"No execution mode enabled for MWS_Connectivity")
    
    print(f"✓ MWS_Connectivity completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='MWS_Connectivity',
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


def execute_MWS_Centroid(**context):
    """
    Execute MWS_Centroid - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: MWS_Centroid")
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
    algo_config = get_active_algorithm_config('MWS_Centroid')
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
        raise ValueError(f"No execution mode enabled for MWS_Centroid")
    
    print(f"✓ MWS_Centroid completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='MWS_Centroid',
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


def execute_Soil_Type(**context):
    """
    Execute Soil_Type - AUTO-GENERATED
    Supports both API and Docker execution with priority-based selection
    """
    print("="*60)
    print("Executing: Soil_Type")
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
    algo_config = get_active_algorithm_config('Soil_Type')
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
        raise ValueError(f"No execution mode enabled for Soil_Type")
    
    print(f"✓ Soil_Type completed successfully")
    print(f"   Output assets ({len(asset_id)}): {asset_id}")
    print(f"   Version: {version}")
    
    # Log to database
    db = STACDDatabase(DB_PATH)
    try:
        dag_record = db.get_dag_by_id(DAG_ID)
        db.log_algorithm_execution(
            dag_uuid=dag_record.dag_uuid,
            algo_type_id='Soil_Type',
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

def fetch_Admin_Boundary_Asset(**context):
    """
    Fetch Admin_Boundary_Asset root dataset from database
    Root datasets are pre-existing and not produced by algorithms.
    """
    print("="*60)
    print("Fetching Root Dataset: Admin_Boundary_Asset")
    print("="*60)
    
    ti = context['ti']
    params = context['params']
    
    # Extract region parameters
    state = params.get('state')
    district = params.get('district')
    block = params.get('block')

    execution_type = params.get('execution_type', 'fullexec')
    updated_dataset = params.get('updated_dataset', '')
    is_being_updated = (execution_type == 'update_dataset' and updated_dataset == 'Admin_Boundary_Asset')

    print(f"🔍 Looking for Admin_Boundary_Asset in region: {state}/{district}/{block}")
    if is_being_updated:
        print(f"⚡ update_dataset mode — will use latest registered version")

    # Query database for root dataset (always gets latest version)
    db = STACDDatabase(DB_PATH)
    try:
        root_dataset = db.get_root_dataset(
            dataset_type_id='Admin_Boundary_Asset',
            state=state,
            district=district,
            block=block
        )
        
        if not root_dataset:
            raise ValueError(f"Root dataset Admin_Boundary_Asset not found for {state}/{district}/{block}")
        
        asset_id = root_dataset.asset_id
        version = root_dataset.version
        print(f"✓ Found Admin_Boundary_Asset version {version}")
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


def register_Agro_Ecological_Asset(**context):
    """Register Agro_Ecological_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Agro_Ecological_Asset")
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

    print(f"Registering Agro_Ecological_Asset")
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
                dataset_type_id="Agro_Ecological_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Agro_Ecological_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Agro_Ecological_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Agro_Ecological_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Agro_Ecological_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Antyodaya_Asset(**context):
    """Register Antyodaya_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Antyodaya_Asset")
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

    print(f"Registering Antyodaya_Asset")
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
                dataset_type_id="Antyodaya_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Antyodaya_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Antyodaya_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Antyodaya_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Antyodaya_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Aquifer_Vector_Asset(**context):
    """Register Aquifer_Vector_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Aquifer_Vector_Asset")
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

    print(f"Registering Aquifer_Vector_Asset")
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
                dataset_type_id="Aquifer_Vector_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Aquifer_Vector_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Aquifer_Vector_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Aquifer_Vector_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Aquifer_Vector_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Canal_Asset(**context):
    """Register Canal_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Canal_Asset")
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

    print(f"Registering Canal_Asset")
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
                dataset_type_id="Canal_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Canal_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Canal_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Canal_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Canal_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Catchment_Area_Asset(**context):
    """Register Catchment_Area_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Catchment_Area_Asset")
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

    print(f"Registering Catchment_Area_Asset")
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
                dataset_type_id="Catchment_Area_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Catchment_Area_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Catchment_Area_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Catchment_Area_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Catchment_Area_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Digital_Elevation_Model_Asset(**context):
    """Register Digital_Elevation_Model_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Digital_Elevation_Model_Asset")
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

    print(f"Registering Digital_Elevation_Model_Asset")
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
                dataset_type_id="Digital_Elevation_Model_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Digital_Elevation_Model_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Digital_Elevation_Model_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Digital_Elevation_Model_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Digital_Elevation_Model_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Dist_to_Drainage_Asset(**context):
    """Register Dist_to_Drainage_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Dist_to_Drainage_Asset")
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

    print(f"Registering Dist_to_Drainage_Asset")
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
                dataset_type_id="Dist_to_Drainage_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Dist_to_Drainage_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Dist_to_Drainage_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Dist_to_Drainage_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Dist_to_Drainage_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Drainage_Density_Asset(**context):
    """Register Drainage_Density_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Drainage_Density_Asset")
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

    print(f"Registering Drainage_Density_Asset")
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
                dataset_type_id="Drainage_Density_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Drainage_Density_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Drainage_Density_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Drainage_Density_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Drainage_Density_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Drainage_Lines_Asset(**context):
    """Register Drainage_Lines_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Drainage_Lines_Asset")
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

    print(f"Registering Drainage_Lines_Asset")
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
                dataset_type_id="Drainage_Lines_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Drainage_Lines_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Drainage_Lines_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Drainage_Lines_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Drainage_Lines_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Facilities_Proximity_Asset(**context):
    """Register Facilities_Proximity_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Facilities_Proximity_Asset")
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

    print(f"Registering Facilities_Proximity_Asset")
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
                dataset_type_id="Facilities_Proximity_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Facilities_Proximity_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Facilities_Proximity_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Facilities_Proximity_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Facilities_Proximity_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Factory_CSR_Asset(**context):
    """Register Factory_CSR_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Factory_CSR_Asset")
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

    print(f"Registering Factory_CSR_Asset")
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
                dataset_type_id="Factory_CSR_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Factory_CSR_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Factory_CSR_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Factory_CSR_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Factory_CSR_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Green_Credit_Asset(**context):
    """Register Green_Credit_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Green_Credit_Asset")
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

    print(f"Registering Green_Credit_Asset")
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
                dataset_type_id="Green_Credit_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Green_Credit_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Green_Credit_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Green_Credit_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Green_Credit_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_LCW_Conflict_Asset(**context):
    """Register LCW_Conflict_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: LCW_Conflict_Asset")
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

    print(f"Registering LCW_Conflict_Asset")
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
                dataset_type_id="LCW_Conflict_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"LCW_Conflict_Asset registered in database")
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
                        'stacd:dataset_type_id': 'LCW_Conflict_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'LCW_Conflict_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "LCW_Conflict_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Livestock_Asset(**context):
    """Register Livestock_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Livestock_Asset")
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

    print(f"Registering Livestock_Asset")
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
                dataset_type_id="Livestock_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Livestock_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Livestock_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Livestock_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Livestock_Asset", instance.instance_id, params)
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


def register_MWS_Centroid_Asset(**context):
    """Register MWS_Centroid_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: MWS_Centroid_Asset")
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

    print(f"Registering MWS_Centroid_Asset")
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
                dataset_type_id="MWS_Centroid_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"MWS_Centroid_Asset registered in database")
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
                        'stacd:dataset_type_id': 'MWS_Centroid_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'MWS_Centroid_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "MWS_Centroid_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_MWS_Connectivity_Asset(**context):
    """Register MWS_Connectivity_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: MWS_Connectivity_Asset")
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

    print(f"Registering MWS_Connectivity_Asset")
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
                dataset_type_id="MWS_Connectivity_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"MWS_Connectivity_Asset registered in database")
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
                        'stacd:dataset_type_id': 'MWS_Connectivity_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'MWS_Connectivity_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "MWS_Connectivity_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Mining_Asset(**context):
    """Register Mining_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Mining_Asset")
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

    print(f"Registering Mining_Asset")
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
                dataset_type_id="Mining_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Mining_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Mining_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Mining_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Mining_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Natural_Depression_Asset(**context):
    """Register Natural_Depression_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Natural_Depression_Asset")
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

    print(f"Registering Natural_Depression_Asset")
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
                dataset_type_id="Natural_Depression_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Natural_Depression_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Natural_Depression_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Natural_Depression_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Natural_Depression_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Restoration_Opportunity_Asset(**context):
    """Register Restoration_Opportunity_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Restoration_Opportunity_Asset")
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

    print(f"Registering Restoration_Opportunity_Asset")
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
                dataset_type_id="Restoration_Opportunity_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Restoration_Opportunity_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Restoration_Opportunity_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Restoration_Opportunity_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Restoration_Opportunity_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_River_Asset(**context):
    """Register River_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: River_Asset")
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

    print(f"Registering River_Asset")
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
                dataset_type_id="River_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"River_Asset registered in database")
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
                        'stacd:dataset_type_id': 'River_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'River_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "River_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_SOGE_Vector_Asset(**context):
    """Register SOGE_Vector_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: SOGE_Vector_Asset")
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

    print(f"Registering SOGE_Vector_Asset")
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
                dataset_type_id="SOGE_Vector_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"SOGE_Vector_Asset registered in database")
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
                        'stacd:dataset_type_id': 'SOGE_Vector_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'SOGE_Vector_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "SOGE_Vector_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Slope_Percentage_Asset(**context):
    """Register Slope_Percentage_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Slope_Percentage_Asset")
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

    print(f"Registering Slope_Percentage_Asset")
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
                dataset_type_id="Slope_Percentage_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Slope_Percentage_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Slope_Percentage_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Slope_Percentage_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Slope_Percentage_Asset", instance.instance_id, params)
            else:
                generate_stac_for_dataset(instance, params, None)
        
    finally:
        db.close()
    
    ti.xcom_push(key="asset_id", value=asset_id)
    ti.xcom_push(key="version", value=version)
    
    return {"status": "success", "asset_id": asset_id, "version": version}

def register_Soil_Type_Asset(**context):
    """Register Soil_Type_Asset dataset AND generate STAC item"""
    print("=" * 60)
    print(f"Registering Dataset: Soil_Type_Asset")
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

    print(f"Registering Soil_Type_Asset")
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
                dataset_type_id="Soil_Type_Asset",
                asset_id=single_asset_id,
                produced_by_algo=producing_algo,
                algo_version=algo_version,
                run_id=run_id,
                meta_info=meta_info_val,
            )
            instances.append(inst)

        instance = instances[-1]

        version = instance.version
        print(f"Soil_Type_Asset registered in database")
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
                        'stacd:dataset_type_id': 'Soil_Type_Asset',
                        'stacd:instance_id': str(instance.instance_id),
                        'stacd:dataset_version': str(version),
                        'stacd:registered_at': str(instance.created_at),
                    })

                    item_id = item.get('id', f'Soil_Type_Asset_{instance.instance_id}')
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
                augment_and_write_stac(stac_spec, "Soil_Type_Asset", instance.instance_id, params)
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
    description="Local-compute pipeline for static layers (independent, dependency-free layers) from local_layer_map.json.",
    schedule_interval=None,
    catchup=False,
    tags=['stacd', 'recompute', 'generic'],
    params={
        'state': 'default_value',
        'district': 'default_value',
        'block': 'default_value',
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

    Aquifer_Vector = PythonOperator(
        task_id='Aquifer_Vector',
        python_callable=execute_Aquifer_Vector,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Livestock = PythonOperator(
        task_id='Livestock',
        python_callable=execute_Livestock,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Antyodaya = PythonOperator(
        task_id='Antyodaya',
        python_callable=execute_Antyodaya,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Drainage_Density = PythonOperator(
        task_id='Drainage_Density',
        python_callable=execute_Drainage_Density,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    River = PythonOperator(
        task_id='River',
        python_callable=execute_River,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Canal = PythonOperator(
        task_id='Canal',
        python_callable=execute_Canal,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Digital_Elevation_Model = PythonOperator(
        task_id='Digital_Elevation_Model',
        python_callable=execute_Digital_Elevation_Model,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Facilities_Proximity = PythonOperator(
        task_id='Facilities_Proximity',
        python_callable=execute_Facilities_Proximity,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Drainage_Lines = PythonOperator(
        task_id='Drainage_Lines',
        python_callable=execute_Drainage_Lines,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Restoration_Opportunity = PythonOperator(
        task_id='Restoration_Opportunity',
        python_callable=execute_Restoration_Opportunity,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    SOGE_Vector = PythonOperator(
        task_id='SOGE_Vector',
        python_callable=execute_SOGE_Vector,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    LCW_Conflict = PythonOperator(
        task_id='LCW_Conflict',
        python_callable=execute_LCW_Conflict,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Agro_Ecological = PythonOperator(
        task_id='Agro_Ecological',
        python_callable=execute_Agro_Ecological,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Factory_CSR = PythonOperator(
        task_id='Factory_CSR',
        python_callable=execute_Factory_CSR,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Green_Credit = PythonOperator(
        task_id='Green_Credit',
        python_callable=execute_Green_Credit,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Mining = PythonOperator(
        task_id='Mining',
        python_callable=execute_Mining,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Natural_Depression = PythonOperator(
        task_id='Natural_Depression',
        python_callable=execute_Natural_Depression,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Dist_to_Drainage = PythonOperator(
        task_id='Dist_to_Drainage',
        python_callable=execute_Dist_to_Drainage,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Catchment_Area = PythonOperator(
        task_id='Catchment_Area',
        python_callable=execute_Catchment_Area,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Slope_Percentage = PythonOperator(
        task_id='Slope_Percentage',
        python_callable=execute_Slope_Percentage,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    MWS_Connectivity = PythonOperator(
        task_id='MWS_Connectivity',
        python_callable=execute_MWS_Connectivity,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    MWS_Centroid = PythonOperator(
        task_id='MWS_Centroid',
        python_callable=execute_MWS_Centroid,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Soil_Type = PythonOperator(
        task_id='Soil_Type',
        python_callable=execute_Soil_Type,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
        on_failure_callback=log_algo_failure
    )

    Admin_Boundary_Asset = PythonOperator(
        task_id='Admin_Boundary_Asset',
        python_callable=fetch_Admin_Boundary_Asset,
        provide_context=True
    )

    Agro_Ecological_Asset = PythonOperator(
        task_id='Agro_Ecological_Asset',
        python_callable=register_Agro_Ecological_Asset,
        provide_context=True
    )

    Antyodaya_Asset = PythonOperator(
        task_id='Antyodaya_Asset',
        python_callable=register_Antyodaya_Asset,
        provide_context=True
    )

    Aquifer_Vector_Asset = PythonOperator(
        task_id='Aquifer_Vector_Asset',
        python_callable=register_Aquifer_Vector_Asset,
        provide_context=True
    )

    Canal_Asset = PythonOperator(
        task_id='Canal_Asset',
        python_callable=register_Canal_Asset,
        provide_context=True
    )

    Catchment_Area_Asset = PythonOperator(
        task_id='Catchment_Area_Asset',
        python_callable=register_Catchment_Area_Asset,
        provide_context=True
    )

    Digital_Elevation_Model_Asset = PythonOperator(
        task_id='Digital_Elevation_Model_Asset',
        python_callable=register_Digital_Elevation_Model_Asset,
        provide_context=True
    )

    Dist_to_Drainage_Asset = PythonOperator(
        task_id='Dist_to_Drainage_Asset',
        python_callable=register_Dist_to_Drainage_Asset,
        provide_context=True
    )

    Drainage_Density_Asset = PythonOperator(
        task_id='Drainage_Density_Asset',
        python_callable=register_Drainage_Density_Asset,
        provide_context=True
    )

    Drainage_Lines_Asset = PythonOperator(
        task_id='Drainage_Lines_Asset',
        python_callable=register_Drainage_Lines_Asset,
        provide_context=True
    )

    Facilities_Proximity_Asset = PythonOperator(
        task_id='Facilities_Proximity_Asset',
        python_callable=register_Facilities_Proximity_Asset,
        provide_context=True
    )

    Factory_CSR_Asset = PythonOperator(
        task_id='Factory_CSR_Asset',
        python_callable=register_Factory_CSR_Asset,
        provide_context=True
    )

    Green_Credit_Asset = PythonOperator(
        task_id='Green_Credit_Asset',
        python_callable=register_Green_Credit_Asset,
        provide_context=True
    )

    LCW_Conflict_Asset = PythonOperator(
        task_id='LCW_Conflict_Asset',
        python_callable=register_LCW_Conflict_Asset,
        provide_context=True
    )

    Livestock_Asset = PythonOperator(
        task_id='Livestock_Asset',
        python_callable=register_Livestock_Asset,
        provide_context=True
    )

    MWS_Boundaries = PythonOperator(
        task_id='MWS_Boundaries',
        python_callable=fetch_MWS_Boundaries,
        provide_context=True
    )

    MWS_Centroid_Asset = PythonOperator(
        task_id='MWS_Centroid_Asset',
        python_callable=register_MWS_Centroid_Asset,
        provide_context=True
    )

    MWS_Connectivity_Asset = PythonOperator(
        task_id='MWS_Connectivity_Asset',
        python_callable=register_MWS_Connectivity_Asset,
        provide_context=True
    )

    Mining_Asset = PythonOperator(
        task_id='Mining_Asset',
        python_callable=register_Mining_Asset,
        provide_context=True
    )

    Natural_Depression_Asset = PythonOperator(
        task_id='Natural_Depression_Asset',
        python_callable=register_Natural_Depression_Asset,
        provide_context=True
    )

    Restoration_Opportunity_Asset = PythonOperator(
        task_id='Restoration_Opportunity_Asset',
        python_callable=register_Restoration_Opportunity_Asset,
        provide_context=True
    )

    River_Asset = PythonOperator(
        task_id='River_Asset',
        python_callable=register_River_Asset,
        provide_context=True
    )

    SOGE_Vector_Asset = PythonOperator(
        task_id='SOGE_Vector_Asset',
        python_callable=register_SOGE_Vector_Asset,
        provide_context=True
    )

    Slope_Percentage_Asset = PythonOperator(
        task_id='Slope_Percentage_Asset',
        python_callable=register_Slope_Percentage_Asset,
        provide_context=True
    )

    Soil_Type_Asset = PythonOperator(
        task_id='Soil_Type_Asset',
        python_callable=register_Soil_Type_Asset,
        provide_context=True
    )

    # ============================================================================
    # DEPENDENCIES (Generated from YAML input_datasets and outputs)
    # ============================================================================

    # Branch connects to all algorithms (selective execution)
    branch_task >> Aquifer_Vector
    branch_task >> Livestock
    branch_task >> Antyodaya
    branch_task >> Drainage_Density
    branch_task >> River
    branch_task >> Canal
    branch_task >> Digital_Elevation_Model
    branch_task >> Facilities_Proximity
    branch_task >> Drainage_Lines
    branch_task >> Restoration_Opportunity
    branch_task >> SOGE_Vector
    branch_task >> LCW_Conflict
    branch_task >> Agro_Ecological
    branch_task >> Factory_CSR
    branch_task >> Green_Credit
    branch_task >> Mining
    branch_task >> Natural_Depression
    branch_task >> Dist_to_Drainage
    branch_task >> Catchment_Area
    branch_task >> Slope_Percentage
    branch_task >> MWS_Connectivity
    branch_task >> MWS_Centroid
    branch_task >> Soil_Type

 # Branch connects to all root datasets (for update_dataset support)
    branch_task >> Admin_Boundary_Asset
    branch_task >> MWS_Boundaries

    # Algorithm -> Dataset dependencies (outputs)
    Aquifer_Vector >> Aquifer_Vector_Asset
    Livestock >> Livestock_Asset
    Antyodaya >> Antyodaya_Asset
    Drainage_Density >> Drainage_Density_Asset
    River >> River_Asset
    Canal >> Canal_Asset
    Digital_Elevation_Model >> Digital_Elevation_Model_Asset
    Facilities_Proximity >> Facilities_Proximity_Asset
    Drainage_Lines >> Drainage_Lines_Asset
    Restoration_Opportunity >> Restoration_Opportunity_Asset
    SOGE_Vector >> SOGE_Vector_Asset
    LCW_Conflict >> LCW_Conflict_Asset
    Agro_Ecological >> Agro_Ecological_Asset
    Factory_CSR >> Factory_CSR_Asset
    Green_Credit >> Green_Credit_Asset
    Mining >> Mining_Asset
    Natural_Depression >> Natural_Depression_Asset
    Dist_to_Drainage >> Dist_to_Drainage_Asset
    Catchment_Area >> Catchment_Area_Asset
    Slope_Percentage >> Slope_Percentage_Asset
    MWS_Connectivity >> MWS_Connectivity_Asset
    MWS_Centroid >> MWS_Centroid_Asset
    Soil_Type >> Soil_Type_Asset

    # Dataset -> Algorithm dependencies (inputs)
    MWS_Boundaries >> Aquifer_Vector
    Admin_Boundary_Asset >> Livestock
    Admin_Boundary_Asset >> Antyodaya
    MWS_Boundaries >> Drainage_Density
    MWS_Boundaries >> River
    MWS_Boundaries >> Canal
    MWS_Boundaries >> Digital_Elevation_Model
    Admin_Boundary_Asset >> Facilities_Proximity
    MWS_Boundaries >> Drainage_Lines
    MWS_Boundaries >> Restoration_Opportunity
    MWS_Boundaries >> SOGE_Vector
    MWS_Boundaries >> LCW_Conflict
    MWS_Boundaries >> Agro_Ecological
    MWS_Boundaries >> Factory_CSR
    MWS_Boundaries >> Green_Credit
    MWS_Boundaries >> Mining
    MWS_Boundaries >> Natural_Depression
    MWS_Boundaries >> Dist_to_Drainage
    MWS_Boundaries >> Catchment_Area
    MWS_Boundaries >> Slope_Percentage
    MWS_Boundaries >> MWS_Connectivity
    MWS_Boundaries >> MWS_Centroid
    MWS_Boundaries >> Soil_Type
