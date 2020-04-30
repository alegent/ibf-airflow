import logging
from datetime import datetime, timedelta

import config.s2_msavi as S2MSAVIC
from airflow.operators import (
    MoveFilesOperator,
    SearchFilesOperator,
    GDALInfoEGEOSValidOperator,
    GDALTranslateOperator,
    GDALAddoOperator,
    GSAddMosaicGranule
    # PythonOperator
)

from airflow import DAG

# from geoserver_plugin import publish_product

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
print("Interval: ".format(S2MSAVIC.dag_schedule_interval))
print("ID: {}".format(S2MSAVIC.id))
print("GeoServer: {} @ {}".format(S2MSAVIC.geoserver_user, S2MSAVIC.geoserver_rest_url))
print("Collection:\n  workspace={}\n  layer={}".format(S2MSAVIC.geoserver_workspace, S2MSAVIC.geoserver_layer))
print("#######################")

# DAG definition
dag = DAG(
    S2MSAVIC.id,
    description='DAG for searching, filtering and processing Sentinel 2 ' + S2MSAVIC.id + ' data',
    schedule_interval=S2MSAVIC.dag_schedule_interval,
    catchup=False,
    default_args=default_args
)

# Search Task Operator
"""
    Scan input folder for new granules availability
"""
search_task = SearchFilesOperator(
    task_id='search_product_task',
    src_dir=S2MSAVIC.src_dir,
    filename_filter=S2MSAVIC.filename_filter,
    dag=dag
)

gdal_info_task = GDALInfoEGEOSValidOperator(
    task_id='gdal_info_task',
    get_inputs_from=search_task.task_id,
    env_parameter=S2MSAVIC.env_parameter,
    wrg_dir=S2MSAVIC.wrg_dir,
    dag=dag
)

gdal_translate_task = GDALTranslateOperator(
    task_id='gdal_translate_task',
    get_inputs_from=gdal_info_task.task_id,
    output_type=S2MSAVIC.output_type,
    creation_options=S2MSAVIC.creation_options,
    dag=dag
)

gdal_addo_task = GDALAddoOperator(
    task_id='gdal_addo_task',
    get_inputs_from=gdal_translate_task.task_id,
    resampling_method=S2MSAVIC.resampling_method,
    max_overview_level=S2MSAVIC.max_overview_level,
    compress_overview=S2MSAVIC.compress_overview,
    dag=dag
)

move_translated_files_task = MoveFilesOperator(
    task_id='move_translated_files_task',
    get_inputs_from=gdal_addo_task.task_id,
    src_dir=S2MSAVIC.src_dir,
    dst_dir=S2MSAVIC.dst_dir,
    filename_filter=S2MSAVIC.filename_filter,
    prefix=S2MSAVIC.tx_prefix,
    regex=S2MSAVIC.regex,
    dag=dag
)

mosaic_granules_ingest_task = GSAddMosaicGranule(
    task_id='mosaic_granules_ingest_task',
    get_inputs_from=move_translated_files_task.task_id,
    geoserver_rest_url=S2MSAVIC.geoserver_rest_url,
    gs_user=S2MSAVIC.geoserver_user,
    gs_password=S2MSAVIC.geoserver_password,
    store_name=S2MSAVIC.geoserver_store_name,
    workspace=S2MSAVIC.geoserver_workspace,
    dag=dag
)

## Sentinel-2 Product.zip Operator.
# product_zip_task = ProductZipOperator(
#     task_id='create_product_zip_task',
#     target_dir=S2MSAVIC.download_dir,
#     generated_files=generated_files_list,
#     placeholders=placeholders_list,
#     dag=dag
# )

# curl -vvv -u evoadmin:\! -XPOST -H "Content-type: application/zip" --data-binary
# @/var/data/Sentinel-2/S2_MSI_L1C/download/S2A_MSIL1C_20170909T093031_N0205_R136_T36VUQ_20170909T093032/product.zip
# "http://ows-oda.eoc.dlr.de/geoserver/rest/oseo/collections/SENTINEL2/products"
# publish_task = PythonOperator(
#     task_id="publish_product_task",
#     python_callable=publish_product,
#     op_kwargs={
#         'geoserver_username': S2MSAVIC.geoserver_username,
#         'geoserver_password': S2MSAVIC.geoserver_password,
#         'geoserver_rest_endpoint': '{}/oseo/collections/{}/products'.format(
#             S2MSAVIC.geoserver_rest_url, S2MSAVIC.geoserver_oseo_collection),
#         'get_inputs_from': product_zip_task.task_id,
#     },
#     dag=dag
# )

search_task >> gdal_info_task >> gdal_translate_task >> gdal_addo_task >> move_translated_files_task >> mosaic_granules_ingest_task
