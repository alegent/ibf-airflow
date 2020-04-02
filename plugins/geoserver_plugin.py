import importlib
import logging
import os
import sys

import requests
import six
from airflow.models import XCOM_RETURN_KEY
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from geoserver.catalog import Catalog

importlib.reload(sys)

log = logging.getLogger(__name__)


class GSAddMosaicGranule(BaseOperator):

    @apply_defaults
    def __init__(self, get_inputs_from, geoserver_rest_url, gs_user, gs_password, store_name, workspace=None, *args,
                 **kwargs):
        self.get_inputs_from = get_inputs_from
        self.catalog = Catalog(geoserver_rest_url, gs_user, gs_password)
        self.store_name = store_name
        self.workspace = workspace
        log.info('--------------------GeoServer_PLUGIN Add granule------------')
        super(GSAddMosaicGranule, self).__init__(*args, **kwargs)

    def execute(self, context):
        input_paths = context["task_instance"].xcom_pull(self.get_inputs_from, key=XCOM_RETURN_KEY)

        if input_paths is None:
            log.info("Nothing to process")
            return None

        # If message from XCom is a string with single file path, turn it into a string
        if isinstance(input_paths, six.string_types):
            input_paths = [input_paths]

        for granule in input_paths:
            store = self.catalog.get_store(self.store_name, workspace=self.workspace)
            if store is None:
                log.info("GeoServer Store {} is not accessible!".format(self.store_name))
                return None
            granule = 'file://' + granule
            log.info('Mosaic granule: %s', granule)
            self.catalog.add_granule(granule, store, workspace=self.workspace)


def generate_wfs_cap_dict(product_identifier, gs_workspace, gs_featuretype, gs_wfs_version):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wfs",
        "method": "GET",
        "code": "GetCapabilities",
        "type": "application/xml",
        "href": "${BASE_URL}" + "/{}/{}/ows?service=WFS&request=GetCapabilities&version={}&CQL_FILTER=eoIdentifier='{}'".format(
            gs_workspace,
            gs_featuretype,
            gs_wfs_version,
            product_identifier
        )
    }


def generate_wms_cap_dict(product_identifier, gs_workspace, gs_layer, gs_wms_version):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wms",
        "method": "GET",
        "code": "GetCapabilities",
        "type": "application/xml",
        "href": "${BASE_URL}" + "/{}/{}/ows?service=WMS&request={}&version={}&CQL_FILTER=eoIdentifier='{}'".format(
            gs_workspace,
            gs_layer,
            "GetCapabilities",
            gs_wms_version,
            product_identifier
        )
    }


def generate_wcs_cap_dict(product_identifier, gs_workspace, coverage_id, gs_wcs_version):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wcs",
        "method": "GET",
        "code": "GetCapabilities",
        "type": "application/xml",
        "href": "${BASE_URL}" + "/{}/{}/ows?service=WCS&request=GetCapabilities&version={}&CQL_FILTER=eoIdentifier='{}'".format(
            gs_workspace,
            coverage_id,
            gs_wcs_version,
            product_identifier
        )
    }


def generate_wfs_dict(product_identifier, gs_workspace, gs_featuretype, gs_wfs_format, gs_wfs_version):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wfs",
        "method": "GET",
        "code": "GetFeature",
        "type": "application/json",
        "href": r"${BASE_URL}" + "/{}/ows?service=wfs&version={}&request=GetFeature&typeNames={}:{}&CQL_FILTER=eoIdentifier='{}'&outputFormat={}".format(
            gs_workspace,
            gs_wfs_version,
            gs_workspace,
            gs_featuretype,
            product_identifier,
            gs_wfs_format
        )
    }


def generate_wms_dict(product_identifier, gs_workspace, gs_layer, bbox, gs_wms_width, gs_wms_height, gs_wms_format,
                      gs_wms_version, timestart, timeend):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wms",
        "method": "GET",
        "code": "GetMap",
        "type": gs_wms_format,
        "href": r"${BASE_URL}" + "/{}/{}/ows?service=wms&request=GetMap&version={}&LAYERS={}&BBOX={},{},{},{}&WIDTH={}&HEIGHT={}&FORMAT={}&CQL_FILTER=eoIdentifier='{}'&TIME={}/{}".format(
            gs_workspace,
            gs_layer,
            gs_wms_version,
            gs_layer,
            bbox["long_min"],
            bbox["lat_min"],
            bbox["long_max"],
            bbox["lat_max"],
            gs_wms_width,
            gs_wms_height,
            gs_wms_format,
            product_identifier,
            timestart,
            timeend
        )
    }


def generate_wcs_dict(product_identifier, bbox, gs_workspace, coverage_id, gs_wcs_scale_i, gs_wcs_scale_j,
                      gs_wcs_format, gs_wcs_version):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wcs",
        "method": "GET",
        "code": "GetCoverage",
        "type": gs_wcs_format,
        "href": r"${BASE_URL}" + "/{}/{}/wcs?service=WCS&version={}&coverageId={}&request=GetCoverage&format={}&subset=http://www.opengis.net/def/axis/OGC/0/Long({},{})&subset=http://www.opengis.net/def/axis/OGC/0/Lat({},{})&scaleaxes=i({}),j({})&CQL_FILTER=eoIdentifier='{}'".format(
            gs_workspace,
            coverage_id,
            gs_wcs_version,
            str(gs_workspace + "__" + coverage_id),
            gs_wcs_format,
            bbox["long_min"],
            bbox["long_max"],
            bbox["lat_min"],
            bbox["lat_max"],
            gs_wcs_scale_i,
            gs_wcs_scale_j,
            product_identifier
        )
    }


def create_owslinks_dict(
        product_identifier,
        timestart,
        timeend,
        granule_bbox,
        gs_workspace,
        gs_wms_layer,
        gs_wms_width,
        gs_wms_height,
        gs_wms_format,
        gs_wms_version,
        gs_wfs_featuretype,
        gs_wfs_format,
        gs_wfs_version,
        gs_wcs_coverage_id,
        gs_wcs_scale_i,
        gs_wcs_scale_j,
        gs_wcs_format,
        gs_wcs_version,
):
    dict = {}
    links = []

    links.append(
        generate_wms_cap_dict(
            product_identifier=product_identifier,
            gs_workspace=gs_workspace,
            gs_layer=gs_wms_layer,
            gs_wms_version=gs_wms_version
        )
    )
    links.append(
        generate_wfs_cap_dict(
            product_identifier=product_identifier,
            gs_workspace=gs_workspace,
            gs_featuretype=gs_wfs_featuretype,
            gs_wfs_version=gs_wfs_version
        )
    )
    links.append(
        generate_wcs_cap_dict(
            product_identifier=product_identifier,
            gs_workspace=gs_workspace,
            coverage_id=gs_wcs_coverage_id,
            gs_wcs_version=gs_wcs_version
        )
    )
    links.append(
        generate_wms_dict(
            product_identifier=product_identifier,
            gs_workspace=gs_workspace,
            gs_layer=gs_wms_layer,
            bbox=granule_bbox,
            gs_wms_width=gs_wms_width,
            gs_wms_height=gs_wms_height,
            gs_wms_format=gs_wms_format,
            gs_wms_version=gs_wms_version,
            timestart=timestart,
            timeend=timeend
        )
    )
    links.append(
        generate_wfs_dict(
            product_identifier=product_identifier,
            gs_workspace=gs_workspace,
            gs_featuretype=gs_wfs_featuretype,
            gs_wfs_format=gs_wfs_format,
            gs_wfs_version=gs_wfs_version
        )
    )
    links.append(
        generate_wcs_dict(
            product_identifier=product_identifier,
            bbox=granule_bbox,
            gs_workspace=gs_workspace,
            coverage_id=gs_wcs_coverage_id,
            gs_wcs_format=gs_wcs_format,
            gs_wcs_scale_i=gs_wcs_scale_i,
            gs_wcs_scale_j=gs_wcs_scale_j,
            gs_wcs_version=gs_wcs_version
        )
    )
    dict["links"] = links

    return dict


def publish_product(geoserver_username, geoserver_password, geoserver_rest_endpoint, get_inputs_from, *args, **kwargs):
    # Pull Zip path from XCom
    log.info("publish_product task")
    log.info("""
        geoserver_username: {}
        geoserver_password: ******
        geoserver_rest_endpoint: {}
        """.format(
        geoserver_username,
        geoserver_rest_endpoint
    )
    )
    task_instance = kwargs['ti']

    zip_files = list()
    if get_inputs_from is not None:
        log.info("Getting inputs from: " + get_inputs_from)
        zip_files = task_instance.xcom_pull(task_ids=get_inputs_from, key=XCOM_RETURN_KEY)
    else:
        log.info("Getting inputs from: product_zip_task")
        zip_files = task_instance.xcom_pull('product_zip_task', key='product_zip_paths')

    log.info("zip_file_paths: {}".format(zip_files))
    if zip_files:
        published_ids = list()
        for zip_file in zip_files:
            # POST product.zip
            log.info("Publishing: {}".format(zip_file))
            d = open(zip_file, 'rb').read()
            a = requests.auth.HTTPBasicAuth(geoserver_username, geoserver_password)
            h = {'Content-type': 'application/zip'}

            r = requests.post(geoserver_rest_endpoint,
                              auth=a,
                              data=d,
                              headers=h)

            if r.ok:
                published_ids.append(r.text)
                log.info("Successfully published product '{}' (HTTP {})".format(r.text, r.status_code))
            else:
                log.warn("Error during publishing product '{}' (HTTP {}: {})".format(zip_file, r.status_code, r.text))
        r.raise_for_status()
        return published_ids
    else:
        log.warn("No product.zip found.")
        return list()


def get_published_products(geoserver_username, geoserver_password, geoserver_rest_url, collection_id, *args, **kwargs):
    # This function returns a list of published products ids 
    log.info("get_published_products task")
    log.info("""
        geoserver_username: {}
        geoserver_password: **
        geoserver_rest_url: {}
        collection_id: {}
        """.format(
        geoserver_username,
        geoserver_rest_url,
        collection_id
    )
    )
    a = requests.auth.HTTPBasicAuth(geoserver_username, geoserver_password)
    r = requests.get('{}/oseo/collections/{}/products'.format(geoserver_rest_url, collection_id), auth=a)
    if r.ok:
        published_products_dict = r.json()
        # Here we fill up the already published products id's
        published_products_ids = [item["id"] for item in published_products_dict.values()[0]]
        return published_products_ids
    else:
        r.raise_for_status()


def is_product_published(geoserver_username, geoserver_password, geoserver_rest_url, collection_id, product_id, *args,
                         **kwargs):
    # This function returns True if it found the product was published, False if not found
    log.info("""
        geoserver_username: {}
        geoserver_password: **
        geoserver_rest_endpoint: {}
        collection_id: {}
        product_id: {}
        """.format(
        geoserver_username,
        os.path.join(geoserver_rest_url, "oseo", "collections", collection_id, "products", product_id),
        collection_id,
        product_id
    )
    )
    a = requests.auth.HTTPBasicAuth(geoserver_username, geoserver_password)
    r = requests.get("{}/oseo/collections/{}/products/{}".format(geoserver_rest_url, collection_id, product_id), auth=a)
    if r.status_code == 200:
        return True
    elif r.status_code == 404:
        return False
    else:
        r.raise_for_status()


class GeoServerPlugin(AirflowPlugin):
    name = "GeoServer_plugin"
    operators = [GSAddMosaicGranule]
