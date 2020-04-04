# ibf-airflow

----------------------------------------------------------------------------------------------------------------------------
```
sudo apt install rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
sudo systemctl status rabbitmq-server

sudo rabbitmqctl add_user airflow airflow
sudo rabbitmqctl set_user_tags airflow administrator

sudo rabbitmqctl add_vhost localhost
sudo rabbitmqctl set_permissions -p / airflow ".*" ".*" ".*"
sudo rabbitmqctl set_permissions -p localhost airflow ".*" ".*" ".*"

wget http://127.0.0.1:15672/cli/rabbitmqadmin
chmod +x rabbitmqadmin
sudo ./rabbitmqadmin declare queue --username=airflow --password=********* --vhost=localhost name=airflow_q durable=true
```

Admin UI : http://<airflow_host>:15672

----------------------------------------------------------------------------------------------------------------------------
```
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" >> /etc/apt/sources.list.d/pgdg.list'
sudo wget --no-check-certificate --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

sudo apt update
sudo apt install -y postgresql-11 postgresql-11-postgis-2.5 postgresql-11-postgis-2.5-scripts postgresql-contrib-11 postgresql-client-11

sudo apt update
sudo apt upgrade
sudo apt autoremove --purge

sudo -u postgres createuser -P airflow
sudo -u postgres createdb -O airflow airflow

sudo -u postgres psql -d airflow -c 'CREATE EXTENSION postgis;'
sudo -u postgres psql -d airflow -c 'GRANT ALL ON geometry_columns TO PUBLIC;'
sudo -u postgres psql -d airflow -c 'GRANT ALL ON spatial_ref_sys TO PUBLIC;'
sudo -u postgres psql -d airflow -c 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;'

sudo -u postgres psql -d airflow -c "ALTER ROLE airflow WITH PASSWORD '************'"

sudo vim /etc/postgresql/11/main/pg_hba.conf

  local   all             postgres                                trust
  local   all             all                                     trust

sudo service postgresql restart
sudo systemctl enable postgresql
```
----------------------------------------------------------------------------------------------------------------------------
```
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update

sudo apt install -y python3-pip python3-dev python3-virtualenv python3-venv virtualenvwrapper
sudo apt install -y sudo apt install -y python3.7 python3.7-dev python3.7-venv
sudo apt install -y libxml2 libxml2-dev gettext
sudo apt install -y gdal-bin libxslt1-dev libjpeg-dev libpng-dev libpq-dev libgdal-dev
sudo apt install -y software-properties-common build-essential
sudo apt install -y git unzip gcc zlib1g-dev libgeos-dev libproj-dev
sudo apt install -y sqlite3 spatialite-bin libsqlite3-mod-spatialite
sudo apt install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget

sudo apt-get install build-essential autoconf libtool pkg-config python-opengl python-pil python-pyrex python-pyside.qtopengl idle-python2.7 qt4-dev-tools qt4-designer libqtgui4 libqtcore4 libqt4-xml libqt4-test libqt4-script libqt4-network libqt4-dbus python-qt4 python-qt4-gl libgle3 python-dev

source /usr/share/virtualenvwrapper/virtualenvwrapper.sh
mkvirtualenv --python=/usr/bin/python3.7 airflow

pip install pygdal=="1.11.3.6"
pip install geoserver-restconfig

git clone https://github.com/alanxz/rabbitmq-c.git
cd rabbitmq-c/
mkdir build && cd build
sudo apt install cmake
sudo cmake ..
sudo cmake ..
sudo cmake --build . --config Release --target install
pip uninstall librabbitmq

git clone https://github.com/geosolutions-it/ibf-airflow.git
cd ibf-airflow/

python3 -m pip install librabbitmq --upgrade --no-cache
pip install apache-airflow[postgres,password,mssql,celery,rabbitmq]

export AIRFLOW_HOME=/home/airflow/ibf-airflow

sed -i 's/\/opt\/airflow/\/home\/airflow\/ibf-airflow/g' airflow.cfg

airflow initdb
```
```python
  python
  >>> import airflow
  >>> from airflow import models, settings
  >>> from airflow.contrib.auth.backends.password_auth import PasswordUser
  >>> user = PasswordUser(models.User())
  >>> user.username = '*************'
  >>> user.email = '***********@**************t'
  >>> user.password = '**********'
  >>> user.superuser = True
  >>> session = settings.Session()
  >>> session.add(user)
  >>> session.commit()
  >>> session.close()
  >>> exit()
```
----------------------------------------------------------------------------------------------------------------------------
```
vim dags/config/s2_msavi.py
```
```python
  dag_schedule_interval = '0/10 * * * *'

  src_dir = '/home/nfs_sat_data/satfarming/sentinel2/it/master/_input_EGEOS_MSAVI'
  dst_dir = '/home/nfs_sat_data/satfarming/sentinel2/it/master/MSAVI'

  geoserver_rest_url = "https://areariservata.ibfservizi.it/geoserver/rest"
  geoserver_user = "************"
  geoserver_password = "***********"
  geoserver_workspace = "geonode"
  geoserver_store_name = "MSAVI"
  geoserver_layer = "MSAVI"
```
```
vim dags/config/s2_ndvi.py
```
```python
  dag_schedule_interval = '0/10 * * * *'

  src_dir = '/home/nfs_sat_data/satfarming/sentinel2/it/master/_input_EGEOS_NDVI'
  dst_dir = '/home/nfs_sat_data/satfarming/sentinel2/it/master/NDVI'

  geoserver_rest_url = "https://areariservata.ibfservizi.it/geoserver/rest"
  geoserver_user = "*************"
  geoserver_password = "************"
  geoserver_workspace = "geonode"
  geoserver_store_name = "NDVI"
  geoserver_layer = "NDVI"
```
----------------------------------------------------------------------------------------------------------------------------
```
sudo vim /etc/systemd/system/airflow-scheduler.service
```
```
  [Unit]
  Description=Airflow cron scheduler daemon
  After=network.target

  [Service]
  EnvironmentFile=/home/airflow/ibf-airflow/.env
  User=airflow
  Group=airflow
  Type=simple
  ExecStart=/bin/bash -c 'source /home/airflow/.virtualenvs/airflow/bin/activate && /home/airflow/.virtualenvs/airflow/bin/airflow scheduler'
  Restart=on-failure
  RestartSec=10s

  [Install]
  WantedBy=multi-user.target
```
```
sudo systemctl daemon-reload
sudo systemctl start airflow-scheduler
sudo systemctl status airflow-scheduler
sudo systemctl enable airflow-scheduler
```
----------------------------------------------------------------------------------------------------------------------------
```
sudo vim /etc/systemd/system/airflow-webserver.service
```
```
  [Unit]
  Description=Airflow http webserver daemon
  After=network.target

  [Service]
  EnvironmentFile=/home/airflow/ibf-airflow/.env
  User=airflow
  Group=airflow
  Type=simple
  ExecStart=/bin/bash -c 'source /home/airflow/.virtualenvs/airflow/bin/activate && /home/airflow/.virtualenvs/airflow/bin/airflow webserver'
  Restart=on-failure
  RestartSec=10s

  [Install]
  WantedBy=multi-user.target
```
```
sudo systemctl daemon-reload
sudo systemctl start airflow-webserver
sudo systemctl status airflow-webserver
sudo systemctl enable airflow-webserver
```
----------------------------------------------------------------------------------------------------------------------------
```
sudo vim /etc/systemd/system/airflow-worker.service
```
```
  [Unit]
  Description=Airflow celery worker daemon
  After=network.target

  [Service]
  EnvironmentFile=/home/airflow/ibf-airflow/.env
  User=airflow
  Group=airflow
  Type=simple
  ExecStart=/bin/bash -c 'source /home/airflow/.virtualenvs/airflow/bin/activate && /home/airflow/.virtualenvs/airflow/bin/airflow worker'
  Restart=on-failure
  RestartSec=10s

  [Install]
  WantedBy=multi-user.target
```
```
sudo systemctl daemon-reload
sudo systemctl start airflow-worker
sudo systemctl status airflow-worker
sudo systemctl enable airflow-worker
```
----------------------------------------------------------------------------------------------------------------------------
