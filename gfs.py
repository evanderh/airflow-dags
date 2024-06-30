import os
import time
import json
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime

from airflow import DAG, Dataset
from airflow.decorators import task, task_group
from airflow.models.taskinstance import TaskInstance
from airflow.providers.http.sensors.http import HttpSensor
from osgeo import gdal
import pendulum

from layers import LAYERS
from legend import build_legend

N_FORECASTS = 3
LAYERS_PATH = '/opt/airflow/layers'
BASE_URL = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl'
HOUR_MARGIN = 3

def cycle_hour(logical_date: pendulum.DateTime):
    logical_date = logical_date.subtract(hours=HOUR_MARGIN)
    return f"{(logical_date.hour // 6) * 6:02d}"

def forecast_dt(logical_date: pendulum.DateTime, hour: int):
    logical_date = logical_date.subtract(hours=HOUR_MARGIN)
    logical_date = logical_date.replace(hour=(logical_date.hour // 6) * 6)
    logical_date = logical_date.add(hours=hour)
    return f"{logical_date.strftime('%Y-%m-%dT%H')}"

def cycle_dt(logical_date: pendulum.DateTime):
    logical_date = logical_date.subtract(hours=HOUR_MARGIN)
    logical_date = logical_date.replace(hour=(logical_date.hour // 6) * 6)
    return f"{logical_date.strftime('%Y-%m-%dT%H')}"

gfs_datasets = [
    Dataset(f'/tmp/f{hour:03}',
            extra={ 'hour': hour })
    for hour in range(N_FORECASTS)
]
tiles_datasets = [
    Dataset(f'/tmp/tiles{hour:03}',
            extra={ 'hour': hour })
    for hour in range(N_FORECASTS)
]

with DAG(
    dag_id='gfs_extract',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    user_defined_macros={
        'cycle_hour': cycle_hour,
    },
) as dag:
    gfs_dir = '/gfs.{{ yesterday_ds_nodash }}/{{ cycle_hour(logical_date) }}/atmos'

    is_gfs_available = HttpSensor(
        task_id='is_gfs_available',
        # https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod
        http_conn_id='gfs_api',
        endpoint=gfs_dir,
    )

    previous = is_gfs_available
    for dataset in gfs_datasets:
        @task(outlets=[dataset])
        def download_file(gfs_dir, filename):
            params = {
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
            }
            encoded_params = urllib.parse.urlencode(params)
            query_uri = f"{BASE_URL}?{encoded_params}"
            print(query_uri)
            urllib.request.urlretrieve(query_uri, dataset.uri)
            time.sleep(10)
        
        filename = 'gfs.t{{ cycle_hour(logical_date) }}z.pgrb2.0p25.f%03d' % dataset.extra['hour']
        download = download_file(gfs_dir, filename)
        previous >> download
        previous = download

def get_band(filename, filter_band):
    # https://gdal.org/programs/gdalinfo.html
    info = gdal.Info(filename, format='json')

    def find_band(band):
        metadata = band['metadata']['']
        return (filter_band['element'] == metadata['GRIB_ELEMENT'] and
                filter_band['layer'] == metadata['GRIB_SHORT_NAME'] and
                filter_band['pdtn'] == metadata['GRIB_PDS_PDTN'])

    return next(filter(find_band, info['bands']))

for dataset, tile_dataset in zip(gfs_datasets, tiles_datasets):
    with DAG(
        dag_id=f"gfs_process_{dataset.extra['hour']:03}",
        schedule=[dataset],
        start_date=datetime(2024, 1, 1),
        catchup=False,
        user_defined_macros={
            'forecast_dt': forecast_dt,
            'cycle_dt': cycle_dt,
            'hour': dataset.extra['hour'],
            'layers_path': LAYERS_PATH,
        },
    ) as dag:
        layers_path = '{{ layers_path }}'
        cycle_name = '{{ cycle_dt(logical_date) }}'
        forecast_name = '{{ forecast_dt(logical_date, hour) }}'
        cycle_path = os.path.join(layers_path, cycle_name)
        forecast_path = os.path.join(cycle_path, forecast_name)

        vector_elements = ['UGRD', 'VGRD']
        process_vectors = []
        process_layers = []

        @task.bash
        def create_dirs_task():
            return f'mkdir -p {forecast_path}'

        for element in vector_elements:
            @task_group(group_id=f'process_{element}')
            def process_vector():
                translate_path = f'{dataset.uri}.{element}.t.tif'
                warp_path = f'{dataset.uri}.{element}.w.tif'
                vector_path = f'{dataset.uri}.{element}.json'

                @task
                def translate(element):
                    band = get_band(dataset.uri, {
                        'element': element,
                        'layer': '10-HTGL',
                        'pdtn': '0',
                    })
                    options = f"-of Gtiff -b {band['band']} -a_nodata none"
                    # https://gdal.org/programs/gdal_translate.html
                    gdal.Translate(translate_path,
                                   dataset.uri,
                                   options=options)

                @task.bash
                def warp():
                    # https://gdal.org/programs/gdalwarp.html
                    return f'gdalwarp -r cubicspline -ts 360 181 -overwrite {translate_path} {warp_path}'

                @task.bash
                def get_data():
                    return f'gtiff2json {warp_path} -o {vector_path}'

                @task
                def output():
                    info = gdal.Info(warp_path, format='json')
                    metadata = info['bands'][0]['metadata']['']

                    with open(vector_path, 'r') as f:
                        return {
                            'data': [ round(n, 2) for n in json.loads(f.read()) ],
                            'header': {
                                'scanMode': '0',
                                'refTime': datetime.fromtimestamp(int(metadata['GRIB_REF_TIME'])).isoformat()+'Z',
                                'forecastTime': int(metadata['GRIB_FORECAST_SECONDS']) / 3600,
                                'parameterCategory': metadata['GRIB_PDS_TEMPLATE_NUMBERS'][0],
                                'parameterNumber':   metadata['GRIB_PDS_TEMPLATE_NUMBERS'][2],
                                'nx': info['size'][0],
                                'ny': info['size'][1],
                                'lo1': -180.0,
                                'la1': 90.0,
                                'dx': 1.0,
                                'dy': 1.0
                            }
                        }

                translate(element) >> warp() >> get_data() >> output()

            process_vectors.append(process_vector())
            
        @task
        def combine_vectors(forecast_path, ti: TaskInstance):
            ugrd = ti.xcom_pull(task_ids=f"process_VGRD.output")
            vgrd = ti.xcom_pull(task_ids=f"process_UGRD.output")
            output_path = os.path.join(forecast_path, 'wind_velocity.json')

            with open(output_path, 'w') as f:
                json.dump([ugrd, vgrd], f)

        for layer in LAYERS:
            element = layer['band']['element']

            @task_group(group_id=f'process_{element}')
            def process_layer():
                translate_path = f'{dataset.uri}.{element}.t.tif'
                warp_path = f'{dataset.uri}.{element}.w.tif'
                color_table_path = f'{dataset.uri}.{element}.color.txt'
                shade_path = f'{dataset.uri}.{element}.s.tif'

                @task
                def translate(layer):
                    band = get_band(dataset.uri, layer['band'])
                    options = ' '.join([
                        f'-b {band["band"]}',
                        f'-scale {layer["scale"][0]} {layer["scale"][1]}',
                        '-ot Byte',
                        '-of Gtiff',
                        '-a_nodata none',
                    ])
                    # https://gdal.org/programs/gdal_translate.html
                    gdal.Translate(translate_path,
                                   dataset.uri,
                                   options=options)

                @task.bash
                def warp():
                    # https://gdal.org/programs/gdalwarp.html
                    return f'gdalwarp -r cubicspline -ts 6400 6400 -overwrite {translate_path} {warp_path}'

                @task
                def build_color_table(layer):
                    with open(color_table_path, 'w') as f:
                        scale = layer['scale']
                        scale_range = scale[1] - scale[0]
                        bit_value = scale_range / 255
                        for row in layer['color_scale']:
                            scaled_value = round((row[0] - scale[0]) / bit_value)
                            scaled_row = ' '.join([str(scaled_value), *map(str, row[1:])])
                            f.write(scaled_row + '\n')
                 
                @task
                def shade():
                    # https://gdal.org/programs/gdaldem.html
                    gdal.DEMProcessing(shade_path,
                                       warp_path,
                                       'color-relief',
                                       colorFilename=color_table_path)

                @task.bash
                def generate_tiles(forecast_path, name):
                    return f'gdal2tiles.py -z 3-6 {shade_path} {forecast_path}/{name}'

                @task
                def generate_legend(layer, layers_path):
                    dest_path = os.path.join(layers_path, f"{layer['name']}.png")
                    build_legend(layer, dest_path)

                translate(layer) >> [warp(), build_color_table(layer)] >> shade() >> generate_tiles(forecast_path, layer['name'])
                generate_legend(layer, layers_path)

            process_layers.append(process_layer())

        @task(outlets=[tile_dataset])
        def complete_process(layers_path, cycle_name):
            new_path = os.path.join(layers_path, 'new')
            if os.path.islink(new_path):
                os.remove(new_path)
            os.symlink(cycle_name, new_path)

        create_dirs = create_dirs_task()
        combine = combine_vectors(forecast_path)
        complete = complete_process(layers_path, cycle_name)
        
        create_dirs >> process_vectors >> combine >> complete
        create_dirs >> process_layers >> complete

import shutil
with DAG(
    dag_id='update_cycle',
    schedule=tiles_datasets,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    user_defined_macros={
        'layers_path': LAYERS_PATH,
    },
) as dag:
    @task
    def update_current_cycle(layers_path):
        current_path = os.path.join(layers_path, 'current')
        current_symlink = Path(current_path)
        prev_target = current_symlink.resolve()

        new_path = os.path.join(layers_path, 'new')
        new_symlink = Path(new_path)
        new_target = new_symlink.resolve()

        if prev_target != new_target:
            print(f'Linking {current_path} to {new_target}')
            os.remove(current_path)
            os.symlink(new_target.name, current_path)
            
            print(f'Removing {prev_target}')
            shutil.rmtree(prev_target)

    layers_path = '{{ layers_path }}'
    update_current_cycle(layers_path)
