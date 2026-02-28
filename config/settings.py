"""
config/settings.py
==================
Central configuration for the Radiology ETL Pipeline.
All paths, constants, and validation rules are defined here.
"""

import os

# ── Base paths ────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW        = os.path.join(BASE_DIR, "data", "raw")
DATA_PROCESSED  = os.path.join(BASE_DIR, "data", "processed")
DATA_WAREHOUSE  = os.path.join(BASE_DIR, "data", "warehouse")
LOGS_DIR        = os.path.join(BASE_DIR, "logs")
REPORTS_DIR     = os.path.join(BASE_DIR, "reports")

# ── Database ──────────────────────────────────────────────────────
WAREHOUSE_DB    = os.path.join(DATA_WAREHOUSE, "radiology_warehouse.db")

# ── Source file names (simulated exports) ─────────────────────────
RIS_FILE        = os.path.join(DATA_RAW, "ris_export.csv")
PACS_FILE       = os.path.join(DATA_RAW, "pacs_export.csv")
EPR_FILE        = os.path.join(DATA_RAW, "epr_export.csv")

# ── Watermark file (tracks last successful extraction) ────────────
WATERMARK_FILE  = os.path.join(DATA_PROCESSED, "watermark.json")

# ── Data generation settings ──────────────────────────────────────
NUM_RECORDS     = 500          # Records per source system
SEED            = 42           # Random seed for reproducibility

# ── Modality controlled vocabulary (RIS → Standard mapping) ───────
MODALITY_MAP = {
    # RIS raw codes → Standardised name
    "XR":    "X-Ray",
    "XRAY":  "X-Ray",
    "x-ray": "X-Ray",
    "CT":    "CT Scan",
    "ct":    "CT Scan",
    "CAT":   "CT Scan",
    "MRI":   "MRI",
    "mri":   "MRI",
    "MR":    "MRI",
    "US":    "Ultrasound",
    "ULTR":  "Ultrasound",
    "NM":    "Nuclear Medicine",
    "NMED":  "Nuclear Medicine",
    "FL":    "Fluoroscopy",
    "FLURO": "Fluoroscopy",
    "DXA":   "Bone Density",
    "DEXA":  "Bone Density",
    "MAM":   "Mammography",
    "MAMM":  "Mammography",
}

# ── Validation thresholds ─────────────────────────────────────────
MAX_TURNAROUND_MINS   = 10_080    # 7 days in minutes – flag if exceeded
MIN_TURNAROUND_MINS   = 0         # Must be >= 0
PATIENT_ID_LENGTH     = 8         # Expected NHS-style ID length

# ── Pipeline settings ─────────────────────────────────────────────
MAX_RETRIES           = 3
RETRY_DELAY_SECS      = 2
SCHEDULE_HOUR         = 5         # 05:00 daily run
SCHEDULE_MINUTE       = 0

# ── Security settings ─────────────────────────────────────────────
HASH_ALGORITHM        = "sha256"  # Patient ID pseudonymisation
RBAC_ROLES = {
    "admin":      ["read", "write", "delete", "admin"],
    "engineer":   ["read", "write"],
    "analyst":    ["read"],
    "dashboard":  ["read"],
}

# ── Report settings ───────────────────────────────────────────────
REPORT_FILE     = os.path.join(REPORTS_DIR, "kpi_report.xlsx")
LOG_FILE        = os.path.join(LOGS_DIR, "pipeline.log")
