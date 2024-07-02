from datetime import timedelta


n=4

def cycle_date(dt):
    dt = dt - timedelta(hours=n)
    return f"{dt.strftime('%Y%m%d')}"

def cycle_hour(dt):
    dt = dt - timedelta(hours=n)
    return f"{(dt.hour // 6) * 6:02d}"

def cycle_dt(dt):
    dt = dt - timedelta(hours=n)
    return dt.replace(hour=(dt.hour // 6) * 6,
                            minute=0,
                            second=0,
                            microsecond=0)

