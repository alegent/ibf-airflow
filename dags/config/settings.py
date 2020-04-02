import os
from datetime import datetime, timedelta

#
# Airflow root directory
#
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.dirname(__file__)
    )
)

#
# Paths
#
base_dir = os.getenv('BASE_DIR', '/var/data/')
regions_base_dir = os.path.join(base_dir, 'regions')
repository_base_dir = os.getenv('REPOSITORY_DIR', os.path.join(base_dir, 'repository'))
templates_base_dir = os.getenv('TEMPLATES_DIR', os.path.join(PROJECT_ROOT, 'plugins', 'templates'))

#
# Dates
#
# yesterday at beginning of day
yesterday_start = datetime.now() - timedelta(days=1)
yesterday_start = yesterday_start.replace(hour=0, minute=0, second=0, microsecond=0)
yesterday_start = yesterday_start.isoformat() + 'Z'
# yesterday at end of day
yesterday_end = datetime.now() - timedelta(days=1)
yesterday_end = yesterday_end.replace(hour=23, minute=59, second=59, microsecond=999999)
yesterday_end = yesterday_end.isoformat() + 'Z'
