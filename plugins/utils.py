import fnmatch
import logging
import os
import pprint
import shutil
import zipfile

import config as CFG
from airflow.models import XCOM_RETURN_KEY
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from jinja2 import Environment, FileSystemLoader

log = logging.getLogger(__name__)


class TemplatesResolver:

    def __init__(self):
        dirname = os.path.dirname(os.path.abspath(__file__))
        log = logging.getLogger(__name__)
        log.info(pprint.pformat(CFG.templates_base_dir))
        self.j2_env = Environment(loader=FileSystemLoader(CFG.templates_base_dir))

    def generate_product_abstract(self, product_abstract_metadata_dict):
        return self.j2_env.get_template('product_abstract.html').render(product_abstract_metadata_dict)

    def generate_sentinel1_product_metadata(self, metadata_dict):
        return self.j2_env.get_template('sentinel1_metadata.xml').render(metadata_dict)

    def generate_sentinel2_product_metadata(self, metadata_dict):
        return self.j2_env.get_template('sentinel2_metadata.xml').render(metadata_dict)

    def generate_sentinel2_product_metadata(self, metadata_dict):
        return self.j2_env.get_template('sentinel2_metadata.xml').render(metadata_dict)


class MoveFilesOperator(BaseOperator):

    @apply_defaults
    def __init__(self, src_dir, dst_dir, filter, *args, **kwargs):
        super(MoveFilesOperator, self).__init__(*args, **kwargs)
        self.src_dir = src_dir
        self.dst_dir = dst_dir
        self.filter = filter
        log.info('--------------------MoveFilesOperator------------')

    def execute(self, context):
        log.info('\nsrc_dir={}\ndst_dir={}\nfilter={}'.format(self.src_dir, self.dst_dir, self.filter))
        filenames = fnmatch.filter(os.listdir(self.src_dir), self.filter)
        for filename in filenames:
            filepath = os.path.join(self.src_dir, filename)
            if not os.path.exists(self.dst_dir):
                log.info("Creating directory {}".format(self.dst_dir))
                os.makedirs(self.dst_dir)
            dst_filename = os.path.join(self.dst_dir, os.path.basename(filename))
            log.info("Moving {} to {}".format(filepath, dst_filename))
            # os.rename(filepath, dst_filename)


class ProductZipOperator(BaseOperator):
    """ This class is receiving the meta data files paths from the Sentinel2MetadataOperator then creates the product.zip Later, this class will pass the path of the created product.zip to the next task to publish on Geoserver.

    Args:
        target_dir (str): path of the downloaded product
        generated_files (list): paths of files that are currently supported (product.json, granules.json, thumbnail.jpeg, owsLinks.json)
        placeholders (list): paths of files that are currently not supported (metadata.xml)
        get_inputs_from (str): task id used to fetch input products from xcom
    Returns:
        list: list of zip files paths for all the processed products
    """

    @apply_defaults
    def __init__(
            self,
            target_dir,
            generated_files,
            placeholders,
            get_inputs_from=None,
            *args, **kwargs):
        self.target_dir = target_dir
        self.generated_files = generated_files
        self.placeholders = placeholders
        self.get_inputs_from = get_inputs_from
        super(ProductZipOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        if self.get_inputs_from is not None:
            log.info("Getting inputs from: " + self.get_inputs_from)
            self.downloaded_products = context['task_instance'].xcom_pull(task_ids=self.get_inputs_from,
                                                                          key=XCOM_RETURN_KEY)
        else:
            log.info("Getting inputs from: dhus_metadata_task")
            self.downloaded_products = context['task_instance'].xcom_pull('dhus_metadata_task',
                                                                          key='downloaded_products')

        # stop processing if there are no products
        if self.downloaded_products is None:
            log.info("Nothing to process.")
            return

        product_zip_paths = list()
        for zipf in self.downloaded_products.keys():
            log.info("Product: {}".format(zipf))
            for ph in self.placeholders:
                shutil.copyfile(ph, os.path.join(zipf.strip(".zip"), ph.split("/")[-1]))
            product_zip_path = os.path.join(os.path.join(zipf.strip(".zip"), "product.zip"))
            product_zip = zipfile.ZipFile(product_zip_path, 'w')
            for item in os.listdir(zipf.strip(".zip")):
                if os.path.isfile(os.path.join(zipf.strip(".zip"), item)) and not item.endswith(".zip"):
                    log.info("inside IF")
                    log.info(item)
                    product_zip.write(os.path.join(zipf.strip(".zip"), item), item)
            product_zip.close()
            product_zip_paths.append(product_zip_path)
            context['task_instance'].xcom_push(key='product_zip_path', value=product_zip_path)
        return product_zip_paths


class UtilsPlugin(AirflowPlugin):
    name = "Utils_plugin"
    operators = [MoveFilesOperator, ProductZipOperator]
