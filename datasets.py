from airflow import Dataset

NUM_FORECASTS = 4
HOURS_PER_FORECAST = 3

gfs_datasets = [
    Dataset(f'/tmp/f{hour:03}',
            extra={ 'hour': hour })
    for hour in range(0, NUM_FORECASTS*HOURS_PER_FORECAST, HOURS_PER_FORECAST)
]

tiles_datasets = [
    Dataset(f'/tmp/tiles{hour:03}',
            extra={ 'hour': hour })
    for hour in range(0, NUM_FORECASTS*HOURS_PER_FORECAST, HOURS_PER_FORECAST)
]
