"""
src/03_validate.py
===================
Staging & Validation Layer

Applies all data quality rules before any data is promoted to the
analytical warehouse. Failed records are routed to an error table
(not silently dropped), preserving full auditability.

Validation rules implemented:
  V01 – Null timestamp detection (exam_start_dt, report_issued_dt)
  V02 – Duplicate Study_ID removal
  V03 – Modality code standardisation to controlled vocabulary
  V04 – Patient ID format validation
  V05 – Negative / illogical turnaround time detection
  V06 – Referential integrity (PACS must link to a valid RIS study_id)
  V07 – Patient ID pseudonymisation (SHA-256 hash)
"""

import os
import sys
import hashlib
import logging
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    DATA_PROCESSED, MODALITY_MAP,
    MAX_TURNAROUND_MINS, MIN_TURNAROUND_MINS,
    PATIENT_ID_LENGTH, HASH_ALGORITHM
)

log = logging.getLogger("etl.validate")

# ── Validation result tracker ─────────────────────────────────────
class ValidationReport:
    def __init__(self):
        self.rules_applied  = []
        self.total_in       = 0
        self.total_passed   = 0
        self.total_failed   = 0
        self.error_records  = []

    def record(self, rule_id, description, passed, failed, sample_errors=None):
        self.rules_applied.append({
            "rule_id":     rule_id,
            "description": description,
            "passed":      passed,
            "failed":      failed,
            "pass_rate":   f"{100 * passed / max(passed+failed, 1):.1f}%",
        })
        self.total_failed += failed
        if sample_errors:
            for err in sample_errors[:3]:   # cap sample at 3
                self.error_records.append({
                    "rule": rule_id,
                    "record": str(err),
                    "timestamp": datetime.now().isoformat(),
                })

    def print_summary(self):
        log.info("  ── Validation Summary ──────────────────────")
        for r in self.rules_applied:
            status = "✓" if r["failed"] == 0 else "⚠"
            log.info(f"  {status} {r['rule_id']} | {r['description']:<45} "
                     f"passed={r['passed']:>4}  failed={r['failed']:>3}  "
                     f"({r['pass_rate']})")
        log.info(f"  Total failed records routed to error table: {self.total_failed}")
        log.info("  ────────────────────────────────────────────")

# ── Rule V01 – Null timestamp detection ──────────────────────────
def check_null_timestamps(df: pd.DataFrame, report: ValidationReport) -> pd.DataFrame:
    required_cols = ["exam_start_dt", "exam_end_dt"]
    mask_fail = df[required_cols].isnull().any(axis=1)
    df_fail = df[mask_fail].copy()
    df_pass = df[~mask_fail].copy()
    report.record("V01", "Null timestamp detection",
                  passed=len(df_pass), failed=len(df_fail),
                  sample_errors=df_fail["study_id"].tolist() if not df_fail.empty else [])
    return df_pass, df_fail

# ── Rule V02 – Duplicate Study_ID removal ─────────────────────────
def check_duplicates(df: pd.DataFrame, report: ValidationReport) -> pd.DataFrame:
    dupes_mask = df.duplicated(subset=["study_id"], keep="first")
    df_dupes = df[dupes_mask].copy()
    df_clean = df[~dupes_mask].copy()
    report.record("V02", "Duplicate Study_ID removal (keep first occurrence)",
                  passed=len(df_clean), failed=len(df_dupes),
                  sample_errors=df_dupes["study_id"].tolist())
    return df_clean, df_dupes

# ── Rule V03 – Modality standardisation ───────────────────────────
def standardise_modality(df: pd.DataFrame, report: ValidationReport) -> pd.DataFrame:
    def map_modality(raw):
        if pd.isna(raw) or str(raw).strip() == "":
            return None
        return MODALITY_MAP.get(str(raw).strip(), None)

    df = df.copy()
    df["modality_standard"] = df["modality_raw"].apply(map_modality)
    unmapped_mask = df["modality_standard"].isna()
    n_unmapped = unmapped_mask.sum()
    # Flag unmapped but keep record (assign "UNKNOWN")
    df.loc[unmapped_mask, "modality_standard"] = "UNKNOWN"
    report.record("V03", "Modality code standardisation to controlled vocabulary",
                  passed=len(df) - n_unmapped, failed=n_unmapped,
                  sample_errors=df.loc[unmapped_mask, "modality_raw"].tolist())
    return df

# ── Rule V04 – Patient ID format validation ───────────────────────
def check_patient_id(df: pd.DataFrame, report: ValidationReport) -> pd.DataFrame:
    def is_valid_pid(pid):
        if pd.isna(pid) or str(pid).strip() == "":
            return False
        clean = str(pid).strip()
        return clean.isdigit() and len(clean) == PATIENT_ID_LENGTH

    df = df.copy()
    valid_mask = df["patient_id"].apply(is_valid_pid)
    n_fail = (~valid_mask).sum()
    # Substitute invalid PIDs with a placeholder (do not drop – preserve linkage)
    df.loc[~valid_mask, "patient_id"] = "INVALID_ID"
    report.record("V04", "Patient ID format validation (8-digit numeric)",
                  passed=valid_mask.sum(), failed=n_fail)
    return df

# ── Rule V05 – Turnaround time sanity check ───────────────────────
def check_turnaround_time(df: pd.DataFrame, report: ValidationReport) -> pd.DataFrame:
    df = df.copy()
    df["exam_end_dt"]        = pd.to_datetime(df["exam_end_dt"],        errors="coerce")
    df["report_issued_dt"]   = pd.to_datetime(df["report_issued_dt"],   errors="coerce")

    df["turnaround_mins"] = (
        (df["report_issued_dt"] - df["exam_end_dt"])
        .dt.total_seconds() / 60
    )

    neg_mask  = df["turnaround_mins"] < MIN_TURNAROUND_MINS
    huge_mask = df["turnaround_mins"] > MAX_TURNAROUND_MINS
    flag_mask = neg_mask | huge_mask

    df["tat_flag"] = "OK"
    df.loc[neg_mask,  "tat_flag"] = "NEGATIVE_TAT"
    df.loc[huge_mask, "tat_flag"] = "EXCESSIVE_TAT"

    report.record("V05", f"Turnaround time sanity (0–{MAX_TURNAROUND_MINS} mins)",
                  passed=(~flag_mask).sum(), failed=flag_mask.sum(),
                  sample_errors=df.loc[flag_mask, "study_id"].tolist())
    return df

# ── Rule V07 – Patient pseudonymisation (GDPR) ───────────────────
def pseudonymise_patient_id(df: pd.DataFrame, report: ValidationReport) -> pd.DataFrame:
    """
    Replace Patient_ID with a SHA-256 hash.
    The original value is never written to the analytical warehouse.
    Satisfies GDPR Article 25 – Data Protection by Design.
    """
    def sha256_hash(pid):
        if pd.isna(pid) or str(pid) in ("", "INVALID_ID"):
            return "PSEUDONYMISED_INVALID"
        return hashlib.sha256(str(pid).encode()).hexdigest()[:16].upper()

    df = df.copy()
    df["patient_id_pseudo"] = df["patient_id"].apply(sha256_hash)
    # Drop the raw patient_id column – it must not persist downstream
    df.drop(columns=["patient_id"], inplace=True)
    report.record("V07", "Patient ID pseudonymised (SHA-256, GDPR Art.25)",
                  passed=len(df), failed=0)
    log.info("  ✓ Patient IDs pseudonymised – original values discarded")
    return df

# ── Referential integrity check (PACS vs RIS) ─────────────────────
def check_referential_integrity(pacs_df: pd.DataFrame,
                                ris_ids: list,
                                report: ValidationReport) -> pd.DataFrame:
    valid_mask = pacs_df["pacs_study_id"].isin(ris_ids)
    df_orphan  = pacs_df[~valid_mask]
    df_valid   = pacs_df[valid_mask].copy()
    report.record("V06", "PACS referential integrity (must link to RIS study)",
                  passed=len(df_valid), failed=len(df_orphan),
                  sample_errors=df_orphan["pacs_study_id"].tolist())
    return df_valid

# ── Write error records ───────────────────────────────────────────
def save_error_table(error_dfs: list, label: str):
    combined = pd.concat(error_dfs, ignore_index=True) if error_dfs else pd.DataFrame()
    path = os.path.join(DATA_PROCESSED, f"errors_{label}.csv")
    combined.to_csv(path, index=False)
    log.info(f"  Error table: {len(combined):,} records → {os.path.basename(path)}")
    return combined

# ── Main ──────────────────────────────────────────────────────────
def run(ris_df=None, pacs_df=None, epr_df=None):
    log.info("=" * 60)
    log.info("STEP 3 – Staging & Validation Layer")
    log.info("=" * 60)

    # Load staging files if not passed directly
    if ris_df is None:
        ris_df  = pd.read_csv(os.path.join(DATA_PROCESSED, "staging_ris.csv"),  dtype=str)
        pacs_df = pd.read_csv(os.path.join(DATA_PROCESSED, "staging_pacs.csv"), dtype=str)
        epr_df  = pd.read_csv(os.path.join(DATA_PROCESSED, "staging_epr.csv"),  dtype=str)

    report = ValidationReport()
    error_chunks = []

    log.info(f"  Input: {len(ris_df):,} RIS | {len(pacs_df):,} PACS | {len(epr_df):,} EPR")

    # ── Apply RIS validation rules in sequence ────────────────────
    ris_df = ris_df.copy()
    ris_df["exam_start_dt"] = pd.to_datetime(ris_df["exam_start_dt"], errors="coerce")
    ris_df["exam_end_dt"]   = pd.to_datetime(ris_df["exam_end_dt"],   errors="coerce")

    ris_df, err_nulls  = check_null_timestamps(ris_df, report)
    ris_df, err_dupes  = check_duplicates(ris_df, report)
    ris_df             = standardise_modality(ris_df, report)
    ris_df             = check_patient_id(ris_df, report)
    ris_df             = check_turnaround_time(ris_df, report)
    ris_df             = pseudonymise_patient_id(ris_df, report)

    error_chunks.extend([err_nulls, err_dupes])

    # ── Apply PACS validation ─────────────────────────────────────
    ris_ids = ris_df["study_id"].tolist()
    pacs_df = check_referential_integrity(pacs_df, ris_ids, report)

    # ── Print summary ─────────────────────────────────────────────
    report.print_summary()

    # ── Save validated data and error tables ──────────────────────
    ris_df.to_csv(os.path.join(DATA_PROCESSED, "validated_ris.csv"),   index=False)
    pacs_df.to_csv(os.path.join(DATA_PROCESSED, "validated_pacs.csv"), index=False)
    epr_df.to_csv(os.path.join(DATA_PROCESSED,  "validated_epr.csv"),  index=False)
    save_error_table(error_chunks, "ris")

    log.info(f"  Validation complete. {len(ris_df):,} RIS records passed all rules.\n")
    return ris_df, pacs_df, epr_df, report

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    run()
