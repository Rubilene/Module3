"""
src/validation_helpers.py
==========================
Standalone validation functions extracted for unit testability.
These are called by 03_validate.py and imported by test_pipeline.py.
"""

import hashlib
import logging
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (MODALITY_MAP, MAX_TURNAROUND_MINS,
                               MIN_TURNAROUND_MINS, PATIENT_ID_LENGTH,
                               HASH_ALGORITHM)

log = logging.getLogger("etl.validate")


class ValidationReport:
    def __init__(self):
        self.rules_applied = []
        self.total_failed  = 0
        self.error_records = []

    def record(self, rule_id, description, passed, failed, sample_errors=None):
        self.rules_applied.append({
            "rule_id": rule_id, "description": description,
            "passed": passed, "failed": failed,
        })
        self.total_failed += failed


def check_null_timestamps(df: pd.DataFrame,
                           report: ValidationReport):
    required = ["exam_start_dt", "exam_end_dt"]
    mask_fail = df[required].isnull().any(axis=1)
    df_pass = df[~mask_fail].copy()
    df_fail = df[mask_fail].copy()
    report.record("V01", "Null timestamp detection",
                  len(df_pass), len(df_fail))
    return df_pass, df_fail


def check_duplicates(df: pd.DataFrame,
                      report: ValidationReport):
    dupes_mask = df.duplicated(subset=["study_id"], keep="first")
    df_dupes   = df[dupes_mask].copy()
    df_clean   = df[~dupes_mask].copy()
    report.record("V02", "Duplicate Study_ID removal",
                  len(df_clean), len(df_dupes))
    return df_clean, df_dupes


def standardise_modality(df: pd.DataFrame,
                          report: ValidationReport) -> pd.DataFrame:
    def map_mod(raw):
        if pd.isna(raw) or str(raw).strip() == "":
            return None
        return MODALITY_MAP.get(str(raw).strip(), None)

    df = df.copy()
    df["modality_standard"] = df["modality_raw"].apply(map_mod)
    unmapped = df["modality_standard"].isna()
    df.loc[unmapped, "modality_standard"] = "UNKNOWN"
    report.record("V03", "Modality standardisation",
                  (~unmapped).sum(), unmapped.sum())
    return df


def check_patient_id(df: pd.DataFrame,
                      report: ValidationReport) -> pd.DataFrame:
    def valid(pid):
        if pd.isna(pid) or str(pid).strip() == "":
            return False
        c = str(pid).strip()
        return c.isdigit() and len(c) == PATIENT_ID_LENGTH

    df = df.copy()
    mask = df["patient_id"].apply(valid)
    df.loc[~mask, "patient_id"] = "INVALID_ID"
    report.record("V04", "Patient ID validation",
                  mask.sum(), (~mask).sum())
    return df


def check_turnaround_time(df: pd.DataFrame,
                           report: ValidationReport) -> pd.DataFrame:
    df = df.copy()
    df["exam_end_dt"]      = pd.to_datetime(df["exam_end_dt"],      errors="coerce")
    df["report_issued_dt"] = pd.to_datetime(df["report_issued_dt"], errors="coerce")
    df["turnaround_mins"]  = (
        (df["report_issued_dt"] - df["exam_end_dt"])
        .dt.total_seconds() / 60
    )
    neg  = df["turnaround_mins"] < MIN_TURNAROUND_MINS
    huge = df["turnaround_mins"] > MAX_TURNAROUND_MINS
    df["tat_flag"] = "OK"
    df.loc[neg,  "tat_flag"] = "NEGATIVE_TAT"
    df.loc[huge, "tat_flag"] = "EXCESSIVE_TAT"
    report.record("V05", "Turnaround time sanity",
                  (~(neg | huge)).sum(), (neg | huge).sum())
    return df


def pseudonymise_patient_id(df: pd.DataFrame,
                              report: ValidationReport) -> pd.DataFrame:
    def sha256(pid):
        if pd.isna(pid) or str(pid) in ("", "INVALID_ID"):
            return "PSEUDONYMISED_INVALID"
        return hashlib.sha256(str(pid).encode()).hexdigest()[:16].upper()

    df = df.copy()
    df["patient_id_pseudo"] = df["patient_id"].apply(sha256)
    df.drop(columns=["patient_id"], inplace=True)
    report.record("V07", "Patient pseudonymisation", len(df), 0)
    return df
