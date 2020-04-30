sudo systemctl stop airflow-worker
sudo systemctl stop airflow-webserver
sudo systemctl stop airflow-scheduler

sudo systemctl restart airflow-worker
sudo systemctl restart airflow-webserver
sudo systemctl restart airflow-scheduler
