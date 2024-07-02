import os
import json
import shutil
import tempfile
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.taskinstance import TaskInstance
from osgeo import gdal
import pendulum

from paths import LAYERS_PATH, CYCLE_PATH
from layers import LAYERS, build_color_table
from legend import build_legend
from datasets import gfs_datasets, tiles_datasets
from grib import translate_vector, get_vector_metadata, translate_layer

DT_FORMAT = '%Y-%m-%dT%H'

for dataset, tile_dataset in zip(gfs_datasets, tiles_datasets):
    with DAG(
        dag_id=f"gfs_process_{dataset.extra['hour']:03}",
        schedule=[dataset],
        start_date=datetime(2024, 7, 1),
        catchup=False,
    ) as dag:

        @task(multiple_outputs=True)
        def get_paths(dataset):
            with open(CYCLE_PATH, 'r') as file:
                cycle_dt = pendulum.parse(file.read())
                forecast_dt = cycle_dt.add(hours=dataset.extra['hour'])
                cycle = cycle_dt.strftime(DT_FORMAT)
                forecast = forecast_dt.strftime(DT_FORMAT)
                return {
                    'cycle': cycle,
                    'forecast': forecast,
                    'forecast_path': os.path.join(LAYERS_PATH, cycle, forecast),
                }

        @task.bash
        def create_dirs_task():
            forecast_path = "{{ ti.xcom_pull(task_ids='get_paths', key='forecast_path') }}"
            return f'mkdir -p {forecast_path}'

        process_vectors = []
        for element in ['UGRD', 'VGRD']:
            @task_group(group_id=f'process_{element}')
            def process_vector(dataset, element):
                translate_path = f'{dataset.uri}.{element}.t.tif'
                warp_path = f'{dataset.uri}.{element}.w.tif'
                vector_path = f'{dataset.uri}.{element}.json'

                @task
                def translate(dataset, element):
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

                translate(dataset, element) >> warp() >> get_data() >> output()

            process_vectors.append(process_vector(dataset, element))
            
        @task
        def combine_vectors(ti: TaskInstance):
            forecast_path = ti.xcom_pull(task_ids='get_paths', key='forecast_path')
            print(forecast_path)
            ugrd = ti.xcom_pull(task_ids=f"process_UGRD.output")
            vgrd = ti.xcom_pull(task_ids=f"process_VGRD.output")
            output_path = os.path.join(forecast_path, 'wind_velocity.json')

            print(f"Saving wind velocity to {output_path}")
            with open(output_path, 'w') as f:
                json.dump([ugrd, vgrd], f)

        process_layers = []
        for layer in LAYERS:
            @task_group(group_id=f"process_{layer['band']['element']}")
            def process_layer(dataset, layer):
                element = layer['band']['element']
                translate_path = f'{dataset.uri}.{element}.t.tif'
                warp_path = f'{dataset.uri}.{element}.w.tif'
                color_table_path = f'{element}.color.txt'
                shade_path = f'{dataset.uri}.{element}.s.tif'

                @task
                def translate(dataset, layer):
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
                def generate_tiles(layer):
                    # https://gdal.org/programs/gdal2tiles.html
                    forecast_path = "{{ ti.xcom_pull(task_ids='get_paths', key='forecast_path') }}"
                    dest_path = os.path.join(forecast_path, layer['name'])
                    print(f"Tiling {layer['name']} from {shade_path} to {dest_path}")
                    return f'gdal2tiles.py -z 3-6 {shade_path} {dest_path}'

                @task
                def generate_legend(layer):
                    dest_path = os.path.join(LAYERS_PATH, f"{layer['name']}.png")
                    print(f"Saving legend for {layer['name']} to {dest_path}")
                    build_legend(layer, dest_path)

                shader = shade()
                translate(dataset, layer) >> warp() >> shader >> generate_tiles(layer)
                color_table(layer) >> shader
                generate_legend(layer)

            process_layers.append(process_layer(dataset, layer))

        @task(outlets=[tile_dataset])
        def complete_process(ti):
            new_path = os.path.join(LAYERS_PATH, 'new')
            cycle = ti.xcom_pull(task_ids='get_paths', key='cycle')

            print(f"Linking {new_path} to {cycle}")
            temp_link_path = tempfile.mktemp(dir=LAYERS_PATH)
            os.symlink(cycle, temp_link_path)
            try:
                os.replace(temp_link_path, new_path)
            except:
                if os.path.islink(temp_link_path):
                    os.remove(temp_link_path)
                raise

        create_dirs = create_dirs_task()
        complete = complete_process()
        
        get_paths(dataset) >> create_dirs
        create_dirs >> process_vectors >> combine_vectors() >> complete
        create_dirs >> process_layers >> complete

with DAG(
    dag_id='gfs_load',
    schedule=tiles_datasets,
    start_date=datetime(2024, 1, 1),
    catchup=False,
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
