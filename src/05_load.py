"""
src/05_load.py
===============
Loading Layer – Incremental append to SQLite analytical warehouse.

Simulates loading to Azure Synapse Analytics (cloud data warehouse).
Uses SQLite for the PoC to demonstrate schema design, partitioning
strategy, and incremental load logic without cloud infrastructure.

Loading strategy:
  - Dimension tables: UPSERT (Type 1 SCD – overwrite on change)
  - Fact table:       Incremental APPEND (new records only)
  - Partitioning:     Fact table partitioned by YYYY_MM for performance

RBAC simulation: access roles are enforced at the query level.
"""

import os
import sys
import sqlite3
import logging
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import WAREHOUSE_DB, DATA_PROCESSED, RBAC_ROLES

log = logging.getLogger("etl.load")

# ── Schema DDL ────────────────────────────────────────────────────
DDL_STATEMENTS = {

"Dim_Date": """
CREATE TABLE IF NOT EXISTS Dim_Date (
    date_key       INTEGER PRIMARY KEY,
    calendar_date  TEXT,
    day_name       TEXT,
    day_of_week    INTEGER,
    week_number    INTEGER,
    month_num      INTEGER,
    month_name     TEXT,
    quarter        INTEGER,
    calendar_year  INTEGER,
    financial_year TEXT,
    is_weekend     INTEGER
)""",

"Dim_Modality": """
CREATE TABLE IF NOT EXISTS Dim_Modality (
    modality_key   INTEGER PRIMARY KEY,
    modality_code  TEXT,
    modality_name  TEXT,
    modality_group TEXT
)""",

"Dim_Radiologist": """
CREATE TABLE IF NOT EXISTS Dim_Radiologist (
    radiologist_key INTEGER PRIMARY KEY,
    radiologist_id  TEXT,
    grade           TEXT,
    speciality      TEXT,
    is_active       INTEGER
)""",

"Dim_Location": """
CREATE TABLE IF NOT EXISTS Dim_Location (
    location_key  INTEGER PRIMARY KEY,
    site_code     TEXT,
    department    TEXT,
    trust         TEXT,
    region        TEXT
)""",

"Fact_Radiology": """
CREATE TABLE IF NOT EXISTS Fact_Radiology (
    study_id            TEXT PRIMARY KEY,
    date_key            INTEGER,
    modality_key        INTEGER,
    radiologist_key     INTEGER,
    location_key        INTEGER,
    patient_id_pseudo   TEXT,
    turnaround_mins     REAL,
    study_count         INTEGER DEFAULT 1,
    report_issued_flag  INTEGER DEFAULT 0,
    tat_band            TEXT,
    priority            TEXT,
    referral_source     TEXT,
    tat_flag            TEXT,
    load_timestamp      TEXT,
    partition_key       TEXT,
    FOREIGN KEY (date_key)        REFERENCES Dim_Date(date_key),
    FOREIGN KEY (modality_key)    REFERENCES Dim_Modality(modality_key),
    FOREIGN KEY (radiologist_key) REFERENCES Dim_Radiologist(radiologist_key),
    FOREIGN KEY (location_key)    REFERENCES Dim_Location(location_key)
)""",

"Audit_Log": """
CREATE TABLE IF NOT EXISTS Audit_Log (
    log_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_time     TEXT,
    pipeline_step  TEXT,
    action         TEXT,
    records_in     INTEGER,
    records_out    INTEGER,
    user_account   TEXT,
    status         TEXT,
    notes          TEXT
)""",

"Error_Table": """
CREATE TABLE IF NOT EXISTS Error_Table (
    error_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    study_id       TEXT,
    error_rule     TEXT,
    error_detail   TEXT,
    captured_at    TEXT
)""",
}

# ── Warehouse connection ──────────────────────────────────────────
def get_connection():
    os.makedirs(os.path.dirname(WAREHOUSE_DB), exist_ok=True)
    conn = sqlite3.connect(WAREHOUSE_DB)
    conn.execute("PRAGMA journal_mode=WAL")   # Write-Ahead Logging for concurrency
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

# ── Schema initialisation ─────────────────────────────────────────
def initialise_schema(conn):
    cur = conn.cursor()
    for table_name, ddl in DDL_STATEMENTS.items():
        cur.execute(ddl)
        log.info(f"  Schema: {table_name} – ready")
    conn.commit()

# ── Dimension upsert (Type 1 SCD) ─────────────────────────────────
def upsert_dimension(conn, df: pd.DataFrame, table: str, pk_col: str) -> int:
    """
    Type 1 SCD: INSERT OR REPLACE.
    If the surrogate key already exists, overwrite with new values.
    """
    if df.empty:
        return 0
    df = df.copy()
    df.to_sql(f"_staging_{table}", conn, if_exists="replace", index=False)
    cur = conn.cursor()
    cols = ", ".join(df.columns)
    placeholders = ", ".join([f"s.{c}" for c in df.columns])
    cur.execute(f"""
        INSERT OR REPLACE INTO {table} ({cols})
        SELECT {placeholders}
        FROM _staging_{table} s
    """)
    cur.execute(f"DROP TABLE IF EXISTS _staging_{table}")
    conn.commit()
    log.info(f"  Upserted {len(df):,} rows → {table}")
    return len(df)

# ── Fact table incremental append ─────────────────────────────────
def load_fact_incremental(conn, fact_df: pd.DataFrame) -> dict:
    """
    Append new records to Fact_Radiology.
    Records already present (same study_id) are skipped.
    Adds load_timestamp and partition_key for operational monitoring.
    """
    df = fact_df.copy()
    now = datetime.now().isoformat()
    df["load_timestamp"] = now

    # Partition key (YYYY_MM) for query performance
    if "date_key" in df.columns:
        df["partition_key"] = df["date_key"].astype(str).str[:6].apply(
            lambda x: f"{x[:4]}_{x[4:6]}" if len(str(x)) >= 6 else "UNKNOWN"
        )
    else:
        df["partition_key"] = "UNKNOWN"

    # Find existing study_ids to skip (incremental)
    existing = pd.read_sql("SELECT study_id FROM Fact_Radiology", conn)["study_id"].tolist()
    new_df   = df[~df["study_id"].isin(existing)]
    skipped  = len(df) - len(new_df)

    if not new_df.empty:
        conn.execute("PRAGMA foreign_keys=OFF")
        new_df.to_sql("Fact_Radiology", conn, if_exists="append", index=False)
        conn.execute("PRAGMA foreign_keys=ON")
    conn.commit()

    log.info(f"  Fact_Radiology:  {len(new_df):,} new rows appended, {skipped:,} duplicates skipped")
    return {"inserted": len(new_df), "skipped": skipped}

# ── Audit logging ─────────────────────────────────────────────────
def write_audit_log(conn, step: str, action: str,
                    records_in: int, records_out: int,
                    status: str = "SUCCESS", notes: str = ""):
    conn.execute("""
        INSERT INTO Audit_Log (event_time, pipeline_step, action,
                               records_in, records_out, user_account, status, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (datetime.now().isoformat(), step, action,
          records_in, records_out, "etl_service_account", status, notes))
    conn.commit()

# ── RBAC check simulation ─────────────────────────────────────────
def check_rbac(role: str, required_permission: str) -> bool:
    """
    Simulate RBAC enforcement.
    In production, this is handled by Azure Active Directory + Synapse ACLs.
    """
    permissions = RBAC_ROLES.get(role, [])
    granted = required_permission in permissions
    if not granted:
        log.warning(f"  RBAC DENIED: role='{role}' does not have '{required_permission}' permission")
    return granted

# ── Warehouse summary query ───────────────────────────────────────
def print_warehouse_summary(conn):
    log.info("  ── Warehouse Summary ───────────────────────")
    tables = ["Fact_Radiology", "Dim_Date", "Dim_Modality",
              "Dim_Radiologist", "Dim_Location", "Audit_Log"]
    for t in tables:
        try:
            n = pd.read_sql(f"SELECT COUNT(*) as n FROM {t}", conn)["n"][0]
            log.info(f"  {t:<20}: {n:>6,} rows")
        except Exception:
            pass

    # KPI summary
    kpi = pd.read_sql("""
        SELECT
            modality_name,
            COUNT(*)                                   AS study_count,
            ROUND(AVG(turnaround_mins), 1)             AS avg_tat_mins,
            ROUND(AVG(report_issued_flag) * 100, 1)    AS report_rate_pct
        FROM Fact_Radiology f
        JOIN Dim_Modality m ON f.modality_key = m.modality_key
        GROUP BY modality_name
        ORDER BY study_count DESC
        LIMIT 10
    """, conn)
    log.info("  ── Top Modalities by Volume ─────────────────")
    for _, row in kpi.iterrows():
        log.info(f"  {row['modality_name']:<20} "
                 f"studies={int(row['study_count']):>4}  "
                 f"avg_tat={row['avg_tat_mins']:>7.1f}m  "
                 f"report_rate={row['report_rate_pct']}%")

# ── Main ──────────────────────────────────────────────────────────
def run(fact_df=None, dim_date=None, dim_modality=None,
        dim_radiologist=None, dim_location=None):

    log.info("=" * 60)
    log.info("STEP 5 – Loading to Analytical Warehouse")
    log.info("=" * 60)

    if fact_df is None:
        fact_df         = pd.read_csv(os.path.join(DATA_PROCESSED, "fact_radiology.csv"))
        dim_date        = pd.read_csv(os.path.join(DATA_PROCESSED, "dim_date.csv"))
        dim_modality    = pd.read_csv(os.path.join(DATA_PROCESSED, "dim_modality.csv"))
        dim_radiologist = pd.read_csv(os.path.join(DATA_PROCESSED, "dim_radiologist.csv"))
        dim_location    = pd.read_csv(os.path.join(DATA_PROCESSED, "dim_location.csv"))

    # RBAC check – only engineer+ can write
    if not check_rbac("engineer", "write"):
        raise PermissionError("Insufficient RBAC permissions for warehouse write.")

    conn = get_connection()
    initialise_schema(conn)

    # Load dimensions first (FK constraint order)
    upsert_dimension(conn, dim_date,        "Dim_Date",        "date_key")
    upsert_dimension(conn, dim_modality,    "Dim_Modality",    "modality_key")
    upsert_dimension(conn, dim_radiologist, "Dim_Radiologist", "radiologist_key")
    upsert_dimension(conn, dim_location,    "Dim_Location",    "location_key")

    # Load fact table incrementally
    result = load_fact_incremental(conn, fact_df)

    # Audit trail
    write_audit_log(conn, "LOAD", "FACT_APPEND",
                    records_in=len(fact_df),
                    records_out=result["inserted"],
                    notes=f"skipped={result['skipped']}")

    print_warehouse_summary(conn)
    conn.close()
    log.info("  Warehouse load complete.\n")
    return result

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    run()
