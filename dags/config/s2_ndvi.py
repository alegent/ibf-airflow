#
# DAG
#
# dag_schedule_interval='@hourly'
dag_schedule_interval = '0/10 * * * *'

#
# Input Files
#
id = "S2_NDVI"
env_parameter = 'NDVI'
platformname = 'Sentinel-2 NDVI'

src_dir = '/home/nfs_sat_data/satfarming/sentinel2/it/master_test/_input_EGEOS_NDVI_test'
dst_dir = '/home/nfs_sat_data/satfarming/sentinel2/it/master_test/NDVI_test'
wrg_dir = '/home/nfs_sat_data/satfarming/sentinel2/it/master_test/wrong_EGEOS_satellite_data'
filename_filter = '*.tif'
regex = r'.*\_([a-zA-Z0-9]{6})\_.*'

#
# GDAL Translate
#
tx_prefix = 'translated_'
output_type = 'Int16'
creation_options = {
    "tiled": True,
    "compress": "DEFLATE",
    "blockxsize": 512,
    "blockysize": 512,
}

#
# GDAL Addo
#
resampling_method = 'nearest'
max_overview_level = 64
compress_overview = True

#
# GeoServer
#
geoserver_rest_url = "https://areariservata.ibfservizi.it/geoserver/rest"
geoserver_user = "***************"
geoserver_password = "***********************"
geoserver_workspace = "geonode"
geoserver_store_name = "NDVI"
geoserver_layer = "NDVI"
