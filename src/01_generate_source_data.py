"""
src/01_generate_source_data.py
===============================
Simulates the export of raw data from three radiology source systems:
  - RIS  (Radiology Information System)
  - PACS (Picture Archiving and Communication System)
  - EPR  (Electronic Patient Record)

In a real deployment these would be SQL extractions or API calls.
Here we generate realistic synthetic data to demonstrate the pipeline.
"""

import os
import sys
import csv
import random
import hashlib
import logging
from datetime import datetime, timedelta

# ── Allow imports from project root ───────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    RIS_FILE, PACS_FILE, EPR_FILE,
    DATA_RAW, NUM_RECORDS, SEED, MODALITY_MAP
)

random.seed(SEED)
log = logging.getLogger("etl.generate")

# ── Helper utilities ──────────────────────────────────────────────
def rand_date(start_days_ago=365, end_days_ago=1):
    """Return a random datetime within the past year."""
    start = datetime.now() - timedelta(days=start_days_ago)
    end   = datetime.now() - timedelta(days=end_days_ago)
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def rand_patient_id():
    """Generate a fake NHS-style 8-digit patient ID."""
    return str(random.randint(10000000, 99999999))

def rand_radiologist_id():
    """Generate a radiologist staff ID."""
    prefixes = ["RAD", "DR", "CON"]
    return f"{random.choice(prefixes)}{random.randint(100,999)}"

def rand_site():
    return random.choice(["MAIN", "NORTH", "SOUTH", "WEST", "EASTWARD"])

def introduce_errors(value, error_rate=0.06):
    """Randomly introduce dirty data to simulate real-world source quality."""
    if random.random() < error_rate:
        choices = [None, "", value, value]   # None = missing, "" = empty
        return random.choice(choices)
    return value

# ── RIS Generator ─────────────────────────────────────────────────
def generate_ris(n):
    """
    Radiology Information System export.
    Contains: study scheduling, modality, radiologist, timestamps.
    """
    raw_modalities = list(MODALITY_MAP.keys())  # Intentionally messy codes
    records = []

    for i in range(n):
        study_id      = f"STU{i+1:06d}"
        patient_id    = introduce_errors(rand_patient_id(), 0.04)
        exam_date     = rand_date()
        exam_end      = exam_date + timedelta(minutes=random.randint(10, 90))
        report_time   = exam_end  + timedelta(minutes=random.randint(30, 2000))
        modality_raw  = introduce_errors(random.choice(raw_modalities), 0.07)
        radiologist   = introduce_errors(rand_radiologist_id(), 0.03)
        site          = rand_site()
        priority      = random.choice(["ROUTINE", "URGENT", "STAT"])

        # Inject some timestamp errors (report before exam end)
        if random.random() < 0.04:
            report_time = exam_end - timedelta(minutes=random.randint(1, 60))

        records.append({
            "study_id":          study_id,
            "patient_id":        patient_id,
            "exam_start_dt":     exam_date.strftime("%Y-%m-%d %H:%M:%S"),
            "exam_end_dt":       exam_end.strftime("%Y-%m-%d %H:%M:%S"),
            "report_issued_dt":  introduce_errors(report_time.strftime("%Y-%m-%d %H:%M:%S"), 0.05),
            "modality_raw":      modality_raw,
            "radiologist_id":    radiologist,
            "site_code":         site,
            "priority":          priority,
            "referral_source":   random.choice(["GP", "A&E", "OUTPATIENT", "INPATIENT"]),
        })

    # Inject deliberate duplicates (approx 3%)
    dupes = random.sample(records, int(n * 0.03))
    for d in dupes:
        records.append(d.copy())

    return records

# ── PACS Generator ────────────────────────────────────────────────
def generate_pacs(n, ris_records):
    """
    PACS export – links to RIS via study_id.
    Contains: DICOM metadata, image file info, acquisition details.
    """
    records = []
    ris_ids = [r["study_id"] for r in ris_records[:n]]

    for study_id in ris_ids:
        records.append({
            "pacs_study_id":     study_id,
            "accession_number":  f"ACC{random.randint(100000, 999999)}",
            "num_images":        random.randint(1, 800),
            "image_size_mb":     round(random.uniform(0.5, 1200.0), 2),
            "modality_dicom":    introduce_errors(random.choice(["CR","CT","MR","US","NM","DX","MG","RF"]), 0.05),
            "acquisition_dt":    rand_date().strftime("%Y-%m-%d %H:%M:%S"),
            "storage_path":      f"/dicom/archive/{study_id[:3]}/{study_id}",
            "dicom_status":      random.choice(["COMPLETE","COMPLETE","COMPLETE","INCOMPLETE","ERROR"]),
        })

    return records

# ── EPR Generator ─────────────────────────────────────────────────
def generate_epr(n, ris_records):
    """
    Electronic Patient Record export.
    Contains: patient demographics (to be pseudonymised downstream).
    """
    records = []
    seen_pids = {}

    for r in ris_records[:n]:
        pid = r.get("patient_id") or rand_patient_id()
        if pid not in seen_pids:
            dob_year = random.randint(1940, 2005)
            seen_pids[pid] = {
                "patient_id":    pid,
                "date_of_birth": f"{dob_year}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                "gender":        random.choice(["M","F","U"]),
                "gp_practice":   f"GP{random.randint(1000,9999)}",
                "trust_code":    random.choice(["RGT","RWE","RTG","RXP","RA7"]),
                "nhs_number":    introduce_errors(str(random.randint(1000000000, 9999999999)), 0.04),
                "ethnicity":     random.choice(["A","B","C","D","E","F","M","N","P","S","Z"]),
            }
        records.append({"study_id": r["study_id"], **seen_pids[pid]})

    return records

# ── Write CSV ─────────────────────────────────────────────────────
def write_csv(records, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if not records:
        return
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    log.info(f"  Written {len(records):,} records → {os.path.basename(filepath)}")

# ── Main ──────────────────────────────────────────────────────────
def run(n=NUM_RECORDS):
    log.info("=" * 60)
    log.info("STEP 1 – Generating source data")
    log.info("=" * 60)

    ris_data  = generate_ris(n)
    pacs_data = generate_pacs(n, ris_data)
    epr_data  = generate_epr(n, ris_data)

    write_csv(ris_data,  RIS_FILE)
    write_csv(pacs_data, PACS_FILE)
    write_csv(epr_data,  EPR_FILE)

    log.info(f"  RIS  : {len(ris_data):,} records (incl. deliberate duplicates & nulls)")
    log.info(f"  PACS : {len(pacs_data):,} records")
    log.info(f"  EPR  : {len(epr_data):,} records")
    log.info("  Source data generation complete.\n")
    return ris_data, pacs_data, epr_data

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    run()
