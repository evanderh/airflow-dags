import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.sensors.http import HttpSensor

from datasets import gfs_datasets
from paths import CYCLE_PATH
from macros import cycle_hour, cycle_date, cycle_dt


# gfs_api url: https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod
BASE_URL = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl'


with DAG(
    dag_id='gfs_extract',
    start_date=datetime(2024, 7, 1),
    schedule=timedelta(hours=6),
    catchup=False,
    user_defined_macros={
        'cycle_hour': cycle_hour,
        'cycle_date': cycle_date,
    },
) as dag:
    is_gfs_available = HttpSensor(
        task_id='is_gfs_available',
        http_conn_id='gfs_api',
        endpoint= '/gfs.{{ cycle_date(dag_run.start_date) }}/{{ cycle_hour(dag_run.start_date) }}/atmos',
        response_error_codes_allowlist=['404', '403'],
        response_check=lambda response: response.status_code == 200,
        timeout=7200,
        poke_interval=60,
    )

    @task
    def save_cycle_dt(*, dag_run):
        print(dag_run.start_date)
        cycle = cycle_dt(dag_run.start_date)
        print(f'Loading {cycle}')
        with open(CYCLE_PATH, 'w') as file:
            file.write(str(cycle))

    is_gfs_available >> save_cycle_dt()

    previous = is_gfs_available
    for dataset in gfs_datasets:
        @task(
            task_id=f"download_{dataset.extra['hour']}",
            outlets=[dataset]
        )
        def download_file(*, dataset, dag_run):
            print(dag_run.start_date)
            gfs_cycle_date = cycle_date(dag_run.start_date)
            gfs_cycle_hour = cycle_hour(dag_run.start_date)
            hour = dataset.extra['hour']

            dir = f"/gfs.{gfs_cycle_date}/{gfs_cycle_hour}/atmos"
            file = f"gfs.t{gfs_cycle_hour}z.pgrb2.0p25.f{hour:03}"
            print(f"Downloading {file} from {dir}")
            encoded_params = urllib.parse.urlencode({
                'dir': dir,
                'file': file,
                'var_PRATE': 'on',
                'var_TMP': 'on',
                'var_UGRD': 'on',
                'var_VGRD': 'on',
                'var_PRMSL': 'on',
                'var_RH': 'on',
                'var_GUST': 'on',
                'lev_2_m_above_ground': 'on',
                'lev_10_m_above_ground': 'on',
                'lev_mean_sea_level': 'on',
                'lev_surface': 'on',
            })
            query_uri = f"{BASE_URL}?{encoded_params}"
            print(query_uri)
            urllib.request.urlretrieve(query_uri, dataset.uri)

            # 10s wait to prevent excessive requests
            time.sleep(10)
        
        # download in series
        download = download_file(dataset=dataset)
        previous >> download
        previous = download
