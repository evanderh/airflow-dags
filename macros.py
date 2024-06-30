import pendulum

HOUR_MARGIN = 3

def cycle_date(logical_date: pendulum.DateTime):
    logical_date = logical_date.subtract(hours=HOUR_MARGIN)
    return f"{logical_date.strftime('%Y%m%d')}"

def cycle_hour(logical_date: pendulum.DateTime):
    logical_date = logical_date.subtract(hours=HOUR_MARGIN)
    return f"{(logical_date.hour // 6) * 6:02d}"

def forecast_ts(logical_date: pendulum.DateTime, hour: int):
    logical_date = logical_date.subtract(hours=HOUR_MARGIN)
    logical_date = logical_date.replace(hour=(logical_date.hour // 6) * 6)
    logical_date = logical_date.add(hours=hour)
    return f"{logical_date.strftime('%Y-%m-%dT%H')}"

def cycle_ts(logical_date: pendulum.DateTime):
    logical_date = logical_date.subtract(hours=HOUR_MARGIN)
    logical_date = logical_date.replace(hour=(logical_date.hour // 6) * 6)
    return f"{logical_date.strftime('%Y-%m-%dT%H')}"

