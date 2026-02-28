"""
src/04_transform.py
====================
Transformation Engine

Converts validated staging data into a dimensional star schema:
  - Dim_Date        – calendar dimension
  - Dim_Modality    – modality dimension
  - Dim_Radiologist – radiologist dimension
  - Dim_Location    – site/location dimension
  - Fact_Radiology  – central fact table with KPI metrics

Key transformations:
  T01 – Surrogate key generation for all dimensions
  T02 – Date dimension population (calendar attributes)
  T03 – KPI derivation (turnaround_mins, study_count, productivity)
  T04 – Type 1 SCD merge for dimension tables
  T05 – Fact table construction with foreign key linkage
"""

import os
import sys
import logging
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATA_PROCESSED

log = logging.getLogger("etl.transform")

# ── Surrogate key generator ───────────────────────────────────────
def make_surrogate_key(value: str) -> int:
    """Deterministic surrogate key from string value using hash."""
    return int(hashlib.md5(str(value).encode()).hexdigest()[:8], 16)

# ── T02 – Date Dimension ──────────────────────────────────────────
def build_dim_date(dates: pd.Series) -> pd.DataFrame:
    """
    Builds a calendar date dimension from a series of datetimes.
    Includes financial year logic aligned to NHS April–March FY.
    """
    unique_dates = pd.to_datetime(dates.dropna()).dt.date.unique()
    rows = []
    for d in unique_dates:
        dt = pd.Timestamp(d)
        fy_start = dt.year if dt.month >= 4 else dt.year - 1
        rows.append({
            "date_key":      int(dt.strftime("%Y%m%d")),
            "calendar_date": d,
            "day_name":      dt.strftime("%A"),
            "day_of_week":   dt.dayofweek + 1,   # Mon=1
            "week_number":   dt.isocalendar()[1],
            "month_num":     dt.month,
            "month_name":    dt.strftime("%B"),
            "quarter":       dt.quarter,
            "calendar_year": dt.year,
            "financial_year": f"FY{fy_start}/{str(fy_start+1)[2:]}",
            "is_weekend":    int(dt.dayofweek >= 5),
        })
    df = pd.DataFrame(rows).drop_duplicates(subset=["date_key"])
    log.info(f"  Dim_Date:        {len(df):,} rows")
    return df

# ── T01 – Modality Dimension ──────────────────────────────────────
def build_dim_modality(ris_df: pd.DataFrame) -> pd.DataFrame:
    modality_groups = {
        "X-Ray":          "Plain Film",
        "CT Scan":        "Cross-Sectional",
        "MRI":            "Cross-Sectional",
        "Ultrasound":     "Ultrasound",
        "Nuclear Medicine":"Nuclear",
        "Fluoroscopy":    "Interventional",
        "Bone Density":   "Plain Film",
        "Mammography":    "Plain Film",
        "UNKNOWN":        "Unclassified",
    }
    unique = ris_df["modality_standard"].dropna().unique()
    rows = []
    for mod in sorted(unique):
        rows.append({
            "modality_key":   make_surrogate_key(mod),
            "modality_code":  mod.replace(" ", "_").upper()[:10],
            "modality_name":  mod,
            "modality_group": modality_groups.get(mod, "Other"),
        })
    df = pd.DataFrame(rows).drop_duplicates(subset=["modality_key"])
    log.info(f"  Dim_Modality:    {len(df):,} rows")
    return df

# ── T01 – Radiologist Dimension ───────────────────────────────────
def build_dim_radiologist(ris_df: pd.DataFrame) -> pd.DataFrame:
    grades = {"RAD": "Registrar", "DR": "Consultant", "CON": "Consultant"}
    unique = ris_df["radiologist_id"].dropna().unique()
    rows = []
    for rid in sorted(unique):
        prefix = str(rid)[:3].upper()
        rows.append({
            "radiologist_key": make_surrogate_key(rid),
            "radiologist_id":  rid,
            "grade":           grades.get(prefix, "Unknown"),
            "speciality":      "Radiology",
            "is_active":       1,
        })
    df = pd.DataFrame(rows).drop_duplicates(subset=["radiologist_key"])
    log.info(f"  Dim_Radiologist: {len(df):,} rows")
    return df

# ── T01 – Location Dimension ──────────────────────────────────────
def build_dim_location(ris_df: pd.DataFrame) -> pd.DataFrame:
    dept_map = {
        "MAIN":     ("ESH",     "East Surrey Hospitals"),
        "CR":    ("CR",  "Crawley Hospitals"),
        "HR":    ("HR",  "Horsham Hospitals"),
        "CT":     ("CT", "Caterham Hospitals"),
        "RH": ("RH",     "Redhill CDC"),
    }
    unique = ris_df["site_code"].dropna().unique()
    rows = []
    for site in sorted(unique):
        dept, trust = dept_map.get(site, (site, "Unknown Trust"))
        rows.append({
            "location_key": make_surrogate_key(site),
            "site_code":    site,
            "department":   dept,
            "trust":        trust,
            "region":       "Surrey and Surrey ",
        })
    df = pd.DataFrame(rows).drop_duplicates(subset=["location_key"])
    log.info(f"  Dim_Location:    {len(df):,} rows")
    return df

# ── T03 / T05 – Fact Table Construction ──────────────────────────
def build_fact_radiology(ris_df: pd.DataFrame,
                          dim_date: pd.DataFrame,
                          dim_modality: pd.DataFrame,
                          dim_radiologist: pd.DataFrame,
                          dim_location: pd.DataFrame) -> pd.DataFrame:
    """
    Central fact table construction.
    Each row = one radiology study with KPI metrics and FK references.
    """
    df = ris_df.copy()

    # ── Date keys ─────────────────────────────────────────────────
    df["exam_start_dt"]  = pd.to_datetime(df["exam_start_dt"], errors="coerce")
    df["exam_end_dt"]    = pd.to_datetime(df["exam_end_dt"],   errors="coerce")
    df["date_key"] = df["exam_start_dt"].dt.strftime("%Y%m%d").astype("Int64")

    # ── Dimension surrogate keys ───────────────────────────────────
    df["modality_key"]     = df["modality_standard"].apply(make_surrogate_key)
    df["radiologist_key"]  = df["radiologist_id"].apply(
        lambda x: make_surrogate_key(x) if pd.notna(x) else -1)
    df["location_key"]     = df["site_code"].apply(make_surrogate_key)

    # ── KPI metrics ────────────────────────────────────────────────
    # Turnaround time (already computed in validation)
    if "turnaround_mins" not in df.columns:
        df["report_issued_dt"] = pd.to_datetime(df["report_issued_dt"], errors="coerce")
        df["turnaround_mins"]  = (
            (df["report_issued_dt"] - df["exam_end_dt"])
            .dt.total_seconds() / 60
        )

    # Study count (additive measure – always 1 per row)
    df["study_count"] = 1

    # Report issued flag
    df["report_issued_flag"] = df["report_issued_dt"].notna().astype(int) \
        if "report_issued_dt" in df.columns else 0

    # Turnaround band (KPI grouping)
    def tat_band(mins):
        if pd.isna(mins) or mins < 0:
            return "Invalid"
        elif mins <= 60:
            return "< 1 hour"
        elif mins <= 240:
            return "1–4 hours"
        elif mins <= 1440:
            return "4–24 hours"
        elif mins <= 4320:
            return "1–3 days"
        else:
            return "> 3 days"

    df["tat_band"] = df["turnaround_mins"].apply(tat_band)

    # ── Select final fact columns ──────────────────────────────────
    fact_cols = [
        "study_id",
        "date_key",
        "modality_key",
        "radiologist_key",
        "location_key",
        "patient_id_pseudo",
        "turnaround_mins",
        "study_count",
        "report_issued_flag",
        "tat_band",
        "priority",
        "referral_source",
        "tat_flag",
    ]
    existing = [c for c in fact_cols if c in df.columns]
    fact_df = df[existing].copy()

    # ── Data type enforcement ─────────────────────────────────────
    fact_df["turnaround_mins"]    = pd.to_numeric(fact_df["turnaround_mins"],    errors="coerce")
    fact_df["report_issued_flag"] = pd.to_numeric(fact_df["report_issued_flag"], errors="coerce").fillna(0).astype(int)
    fact_df["study_count"]        = 1

    log.info(f"  Fact_Radiology:  {len(fact_df):,} rows")
    return fact_df

# ── Main ──────────────────────────────────────────────────────────
def run(ris_df=None, pacs_df=None, epr_df=None):
    log.info("=" * 60)
    log.info("STEP 4 – Transformation Engine")
    log.info("=" * 60)

    if ris_df is None:
        ris_df = pd.read_csv(os.path.join(DATA_PROCESSED, "validated_ris.csv"))

    # Build dimensions
    dim_date        = build_dim_date(pd.to_datetime(ris_df["exam_start_dt"], errors="coerce"))
    dim_modality    = build_dim_modality(ris_df)
    dim_radiologist = build_dim_radiologist(ris_df)
    dim_location    = build_dim_location(ris_df)

    # Build fact table
    fact_radiology = build_fact_radiology(
        ris_df, dim_date, dim_modality, dim_radiologist, dim_location
    )

    # Persist transformed tables
    dim_date.to_csv(os.path.join(DATA_PROCESSED,        "dim_date.csv"),        index=False)
    dim_modality.to_csv(os.path.join(DATA_PROCESSED,    "dim_modality.csv"),    index=False)
    dim_radiologist.to_csv(os.path.join(DATA_PROCESSED, "dim_radiologist.csv"), index=False)
    dim_location.to_csv(os.path.join(DATA_PROCESSED,    "dim_location.csv"),    index=False)
    fact_radiology.to_csv(os.path.join(DATA_PROCESSED,  "fact_radiology.csv"),  index=False)

    log.info("  Transformation complete. Star schema tables written.\n")
    return fact_radiology, dim_date, dim_modality, dim_radiologist, dim_location

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    run()
