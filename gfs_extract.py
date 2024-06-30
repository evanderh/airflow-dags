import time
import urllib.parse
import urllib.request
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.sensors.http import HttpSensor

from macros import cycle_hour, cycle_date
from datasets import gfs_datasets


# gfs_api url: https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod
BASE_URL = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl'


with DAG(
    dag_id='gfs_extract',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    user_defined_macros={
        'cycle_hour': cycle_hour,
        'cycle_date': cycle_date,
    },
) as dag:
    gfs_cycle_date = '{{ cycle_date(logical_date) }}'
    gfs_cycle_hour = '{{ cycle_hour(logical_date) }}'
    gfs_dir = f'/gfs.{gfs_cycle_date}/{gfs_cycle_hour}/atmos'

    is_gfs_available = HttpSensor(
        task_id='is_gfs_available',
        http_conn_id='gfs_api',
        endpoint=gfs_dir,
        response_error_codes_allowlist=['404', '403'],
    )

    previous = is_gfs_available
    for dataset in gfs_datasets:
        filename = f"gfs.t{gfs_cycle_hour}z.pgrb2.0p25.f{dataset.extra['hour']:03}"

        @task(
            task_id=f"download_{dataset.extra['hour']}",
            outlets=[dataset]
        )
        def download_file(gfs_dir, filename):
            print(f"Downloading {filename} from {gfs_dir}")
            encoded_params = urllib.parse.urlencode({
                'dir': gfs_dir,
                'file': filename,
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
        download = download_file(gfs_dir, filename)
        previous >> download
        previous = download
