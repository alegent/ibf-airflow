import logging
from datetime import datetime, timedelta

import config.s2_ndvi as S2NDVIC
from airflow.operators import (
    MoveFilesOperator,
    SearchFilesOperator,
    GDALInfoEGEOSValidOperator,
    GDALTranslateOperator,
    GDALAddoOperator,
    GSAddMosaicGranule
)

from airflow import DAG

log = logging.getLogger(__name__)

# Settings
seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())
default_args = {
    ##################################################
    # General configuration
    #
    # 'start_date': datetime.now() - timedelta(minutes=1),
    'start_date': seven_days_ago,
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'email': ['alessio.fabiani@geo-solutions.it'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'max_threads': 1,
    'max_active_runs': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    #
}

print("#######################")
print("Interval: ".format(S2NDVIC.dag_schedule_interval))
print("ID: {}".format(S2NDVIC.id))
print("GeoServer: {} @ {}".format(S2NDVIC.geoserver_user, S2NDVIC.geoserver_rest_url))
print("Collection:\n  workspace={}\n  layer={}".format(S2NDVIC.geoserver_workspace, S2NDVIC.geoserver_layer))
print("#######################")

# DAG definition
dag = DAG(
    S2NDVIC.id,
    description='DAG for searching, filtering and processing Sentinel 2 ' + S2NDVIC.id + ' data',
    schedule_interval=S2NDVIC.dag_schedule_interval,
    catchup=False,
    default_args=default_args
)

# Search Task Operator
"""
    Scan input folder for new granules availability
"""
search_task = SearchFilesOperator(
    task_id='search_product_task',
    src_dir=S2NDVIC.src_dir,
    filename_filter=S2NDVIC.filename_filter,
    dag=dag
)

gdal_info_task = GDALInfoEGEOSValidOperator(
    task_id='gdal_info_task',
    get_inputs_from=search_task.task_id,
    env_parameter=S2NDVIC.env_parameter,
    wrg_dir=S2NDVIC.wrg_dir,
    dag=dag
)

gdal_translate_task = GDALTranslateOperator(
    task_id='gdal_translate_task',
    get_inputs_from=gdal_info_task.task_id,
    output_type=S2NDVIC.output_type,
    creation_options=S2NDVIC.creation_options,
    dag=dag
)

gdal_addo_task = GDALAddoOperator(
    task_id='gdal_addo_task',
    get_inputs_from=gdal_translate_task.task_id,
    resampling_method=S2NDVIC.resampling_method,
    max_overview_level=S2NDVIC.max_overview_level,
    compress_overview=S2NDVIC.compress_overview,
    dag=dag
)

move_translated_files_task = MoveFilesOperator(
    task_id='move_translated_files_task',
    get_inputs_from=gdal_addo_task.task_id,
    src_dir=S2NDVIC.src_dir,
    dst_dir=S2NDVIC.dst_dir,
    filename_filter=S2NDVIC.filename_filter,
    prefix=S2NDVIC.tx_prefix,
    regex=S2NDVIC.regex,
    dag=dag
)

mosaic_granules_ingest_task = GSAddMosaicGranule(
    task_id='mosaic_granules_ingest_task',
    get_inputs_from=move_translated_files_task.task_id,
    geoserver_rest_url=S2NDVIC.geoserver_rest_url,
    gs_user=S2NDVIC.geoserver_user,
    gs_password=S2NDVIC.geoserver_password,
    store_name=S2NDVIC.geoserver_store_name,
    workspace=S2NDVIC.geoserver_workspace,
    dag=dag
)

search_task >> gdal_info_task >> gdal_translate_task >> gdal_addo_task >> move_translated_files_task >> mosaic_granules_ingest_task
