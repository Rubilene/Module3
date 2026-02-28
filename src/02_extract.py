"""
src/02_extract.py
==================
Extraction Layer – simulates incremental data extraction from source systems
using watermark timestamps.

In production this would connect to:
  - RIS  via SQL Server JDBC / pyodbc
  - PACS via HL7/DICOM metadata API or SQL
  - EPR  via secure HL7 FHIR REST API

Here we read from the CSV exports generated in step 01.
The watermark mechanism ensures only NEW records are processed on each run,
matching the incremental extraction design described in the PoC report.
"""

import os
import sys
import json
import logging
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    RIS_FILE, PACS_FILE, EPR_FILE,
    DATA_PROCESSED, WATERMARK_FILE
)

log = logging.getLogger("etl.extract")

# ── Watermark management ──────────────────────────────────────────
def load_watermark():
    """
    Load the last successful extraction timestamp.
    If no watermark exists (first run), return epoch start.
    """
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, "r") as f:
            wm = json.load(f)
        log.info(f"  Watermark loaded: {wm}")
        return wm
    log.info("  No watermark found – performing full initial load.")
    return {
        "ris_last_extracted":  "1900-01-01 00:00:00",
        "pacs_last_extracted": "1900-01-01 00:00:00",
        "epr_last_extracted":  "1900-01-01 00:00:00",
    }

def save_watermark(wm: dict):
    """Persist the new high-water marks after a successful extraction."""
    os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        json.dump(wm, f, indent=2)
    log.info(f"  Watermark saved: {wm}")

# ── Extraction functions ──────────────────────────────────────────
def extract_ris(watermark_dt: str) -> pd.DataFrame:
    """
    Extract RIS records created/modified after the watermark timestamp.
    Equivalent to:
        SELECT * FROM RIS.Studies
        WHERE exam_start_dt > :last_extracted
    """
    log.info(f"  Extracting RIS (watermark: {watermark_dt})...")
    df = pd.read_csv(RIS_FILE, dtype=str)
    df["exam_start_dt"] = pd.to_datetime(df["exam_start_dt"], errors="coerce")

    # Incremental filter
    wm = pd.to_datetime(watermark_dt)
    df_new = df[df["exam_start_dt"] > wm].copy()
    log.info(f"  RIS: {len(df):,} total records → {len(df_new):,} new since watermark")
    return df_new

def extract_pacs(watermark_dt: str) -> pd.DataFrame:
    """
    Extract PACS metadata records after the watermark.
    Equivalent to:
        SELECT * FROM PACS.StudyMetadata
        WHERE acquisition_dt > :last_extracted
    """
    log.info(f"  Extracting PACS (watermark: {watermark_dt})...")
    df = pd.read_csv(PACS_FILE, dtype=str)
    df["acquisition_dt"] = pd.to_datetime(df["acquisition_dt"], errors="coerce")

    wm = pd.to_datetime(watermark_dt)
    df_new = df[df["acquisition_dt"] > wm].copy()
    log.info(f"  PACS: {len(df):,} total records → {len(df_new):,} new since watermark")
    return df_new

def extract_epr(ris_study_ids: list) -> pd.DataFrame:
    """
    Extract EPR patient records matching the RIS study IDs.
    EPR has no time-based watermark; we join on study_id to limit scope.
    Equivalent to:
        SELECT * FROM EPR.PatientEpisodes
        WHERE study_id IN (:ris_study_ids)
    """
    log.info(f"  Extracting EPR for {len(ris_study_ids):,} study IDs...")
    df = pd.read_csv(EPR_FILE, dtype=str)
    df_filtered = df[df["study_id"].isin(ris_study_ids)].copy()
    log.info(f"  EPR: {len(df):,} total records → {len(df_filtered):,} matched")
    return df_filtered

# ── Security: log data access audit event ─────────────────────────
def log_access_event(source: str, record_count: int, user: str = "etl_service_account"):
    """
    In production this would write to an immutable audit log store.
    Satisfies GDPR Article 30 – Records of Processing Activities.
    """
    event = {
        "timestamp":    datetime.now().isoformat(),
        "user":         user,
        "action":       "EXTRACT",
        "source":       source,
        "record_count": record_count,
        "status":       "SUCCESS",
    }
    log.info(f"  AUDIT: {event}")

# ── Main ──────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info("STEP 2 – Extraction Layer")
    log.info("=" * 60)

    watermark = load_watermark()

    # Extract from each source
    ris_df  = extract_ris(watermark["ris_last_extracted"])
    pacs_df = extract_pacs(watermark["pacs_last_extracted"])

    ris_ids = ris_df["study_id"].dropna().tolist()
    epr_df  = extract_epr(ris_ids)

    # Audit logging
    log_access_event("RIS",  len(ris_df))
    log_access_event("PACS", len(pacs_df))
    log_access_event("EPR",  len(epr_df))

    # Save staging files
    os.makedirs(DATA_PROCESSED, exist_ok=True)
    ris_df.to_csv(os.path.join(DATA_PROCESSED,  "staging_ris.csv"),  index=False)
    pacs_df.to_csv(os.path.join(DATA_PROCESSED, "staging_pacs.csv"), index=False)
    epr_df.to_csv(os.path.join(DATA_PROCESSED,  "staging_epr.csv"),  index=False)

    # Update watermark to now (only after successful extraction)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_watermark = {
        "ris_last_extracted":  now_str,
        "pacs_last_extracted": now_str,
        "epr_last_extracted":  now_str,
    }
    save_watermark(new_watermark)

    log.info(f"  Extraction complete. Staging files written to data/processed/\n")
    return ris_df, pacs_df, epr_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    run()
