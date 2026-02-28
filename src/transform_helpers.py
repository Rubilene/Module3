"""
src/transform_helpers.py
=========================
Standalone transformation functions for unit testability.
"""
import hashlib
import pandas as pd


def make_surrogate_key(value: str) -> int:
    return int(hashlib.md5(str(value).encode()).hexdigest()[:8], 16)


def tat_band(mins) -> str:
    if mins is None or (hasattr(mins, '__class__') and mins != mins):  # NaN check
        return "Invalid"
    try:
        m = float(mins)
    except (TypeError, ValueError):
        return "Invalid"
    if m < 0:
        return "Invalid"
    elif m <= 60:
        return "< 1 hour"
    elif m <= 240:
        return "1–4 hours"
    elif m <= 1440:
        return "4–24 hours"
    elif m <= 4320:
        return "1–3 days"
    else:
        return "> 3 days"


def build_dim_date(dates: pd.Series) -> pd.DataFrame:
    unique_dates = pd.to_datetime(dates.dropna()).dt.date.unique()
    rows = []
    for d in unique_dates:
        dt = pd.Timestamp(d)
        fy_start = dt.year if dt.month >= 4 else dt.year - 1
        rows.append({
            "date_key":      int(dt.strftime("%Y%m%d")),
            "calendar_date": d,
            "day_name":      dt.strftime("%A"),
            "day_of_week":   dt.dayofweek + 1,
            "week_number":   dt.isocalendar()[1],
            "month_num":     dt.month,
            "month_name":    dt.strftime("%B"),
            "quarter":       dt.quarter,
            "calendar_year": dt.year,
            "financial_year": f"FY{fy_start}/{str(fy_start+1)[2:]}",
            "is_weekend":    int(dt.dayofweek >= 5),
        })
    return pd.DataFrame(rows).drop_duplicates(subset=["date_key"])
