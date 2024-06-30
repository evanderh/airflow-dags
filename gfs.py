import os
import json
import shutil
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.taskinstance import TaskInstance
from osgeo import gdal

from grib import translate_vector, get_vector_metadata, translate_layer
from layers import LAYERS, build_color_table
from legend import build_legend
from macros import forecast_ts, cycle_ts
from datasets import gfs_datasets, tiles_datasets

LAYERS_PATH = '/opt/airflow/layers'

for dataset, tile_dataset in zip(gfs_datasets, tiles_datasets):
    with DAG(
        dag_id=f"gfs_process_{dataset.extra['hour']:03}",
        schedule=[dataset],
        start_date=datetime(2024, 1, 1),
        catchup=False,
        user_defined_macros={
            'forecast_ts': forecast_ts,
            'cycle_ts': cycle_ts,
            'hour': dataset.extra['hour'],
            'layers_path': LAYERS_PATH,
        },
    ) as dag:
        layers_path = '{{ layers_path }}'
        cycle_name = '{{ cycle_ts(logical_date) }}'
        forecast_name = '{{ forecast_ts(logical_date, hour) }}'
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
                    # https://gdal.org/programs/gdal_translate.html
                    print(f"Translating {dataset.uri} to {translate_path}")
                    translate_vector(translate_path, dataset.uri, element)

                @task.bash
                def warp():
                    # https://gdal.org/programs/gdalwarp.html
                    print(f"Warping {translate_path} to {warp_path}")
                    return f'gdalwarp -r cubicspline -ts 360 181 -overwrite {translate_path} {warp_path}'

                @task.bash
                def get_data():
                    # https://www.npmjs.com/package/@weacast/gtiff2json
                    print(f"Getting data from {warp_path} to {vector_path}")
                    return f'gtiff2json {warp_path} -o {vector_path}'

                @task
                def output():
                    print(f"Returning data from {warp_path} and {vector_path}")
                    with open(vector_path, 'r') as f:
                        return {
                            'data': [ round(n, 2) for n in json.loads(f.read()) ],
                            'header': get_vector_metadata(warp_path),
                        }

                translate(element) >> warp() >> get_data() >> output()

            process_vectors.append(process_vector())
            
        @task
        def combine_vectors(forecast_path, ti: TaskInstance):
            ugrd = ti.xcom_pull(task_ids=f"process_UGRD.output")
            vgrd = ti.xcom_pull(task_ids=f"process_VGRD.output")
            output_path = os.path.join(forecast_path, 'wind_velocity.json')

            print(f"Saving wind velocity to {output_path}")
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
                    # https://gdal.org/programs/gdal_translate.html
                    print(f"Translating {dataset.uri} to {translate_path}")
                    translate_layer(translate_path, dataset.uri, layer)

                @task.bash
                def warp():
                    # https://gdal.org/programs/gdalwarp.html
                    print(f"Warping {translate_path} to {warp_path}")
                    return f'gdalwarp -r cubicspline -ts 6400 6400 -overwrite {translate_path} {warp_path}'

                @task
                def color_table(layer):
                    print(f"Saving color table for {layer['name']} to {color_table_path}")
                    build_color_table(color_table_path, layer)
                 
                @task
                def shade():
                    # https://gdal.org/programs/gdaldem.html
                    print(f"Shading {shade_path} from {warp_path} and {color_table_path}")
                    gdal.DEMProcessing(shade_path,
                                       warp_path,
                                       'color-relief',
                                       colorFilename=color_table_path)

                @task.bash
                def generate_tiles(forecast_path, layer_name):
                    # https://gdal.org/programs/gdal2tiles.html
                    dest_path = os.path.join(forecast_path, layer_name)
                    print(f"Tiling {layer['name']} from {shade_path} to {dest_path}")
                    return f'gdal2tiles.py -z 3-6 {shade_path} {dest_path}'

                @task
                def generate_legend(layers_path):
                    name = f"{layer['name']}.png"
                    dest_path = os.path.join(layers_path, name)
                    print(f"Saving legend for {layer['name']} to {dest_path}")
                    build_legend(layer, dest_path)

                shader = shade()
                generate = generate_tiles(forecast_path, layer['name'])
                translate(layer) >> warp() >> shader >> generate
                color_table(layer) >> shader
                generate_legend(layers_path)

            process_layers.append(process_layer())

        @task(outlets=[tile_dataset])
        def complete_process(layers_path, cycle_name):
            new_path = os.path.join(layers_path, 'new')
            if os.path.islink(new_path):
                os.remove(new_path)
            print(f"Linking {new_path} to {cycle_name}")
            os.symlink(cycle_name, new_path)

        create_dirs = create_dirs_task()
        combine = combine_vectors(forecast_path)
        complete = complete_process(layers_path, cycle_name)
        
        create_dirs >> process_vectors >> combine >> complete
        create_dirs >> process_layers >> complete

with DAG(
    dag_id='gfs_load',
    schedule=tiles_datasets,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    user_defined_macros={
        'layers_path': LAYERS_PATH,
    },
) as dag:
    @task
    def update_current_cycle():
        current_path = os.path.join(LAYERS_PATH, 'current')
        current_symlink = Path(current_path)
        prev_target = current_symlink.resolve()
        print(f"Current cycle: {prev_target}")

        new_path = os.path.join(LAYERS_PATH, 'new')
        new_symlink = Path(new_path)
        new_target = new_symlink.resolve()

        print(f'Linking {current_path} to new cycle: {new_target.name}')
        if os.path.islink(current_path):
            os.remove(current_path)
        os.symlink(new_target.name, current_path)
        
        if prev_target != new_target and not os.path.islink(prev_target):
            print(f'Removing prev cycle: {prev_target}')
            shutil.rmtree(prev_target)

    update_current_cycle()
