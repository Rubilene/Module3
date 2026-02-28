"""
run_pipeline.py
================
MAIN ENTRY POINT for the Radiology ETL Pipeline.

Assembles all pipeline steps into the Orchestrator DAG and executes them
in dependency order with full retry logic, audit logging, and KPI reporting.

Usage:
    python run_pipeline.py

This simulates the Apache Airflow 05:00 daily DAG described in the report.
"""

import os
import sys
import logging
from datetime import datetime

# ── Project root on path ──────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import LOG_FILE, LOGS_DIR
from src.orchestrator import PipelineOrchestrator, PipelineTask

# ── Logging setup (file + console) ───────────────────────────────
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("etl.main")

# ── Import pipeline step functions ────────────────────────────────
from src import (
    generate_source_data,
    extract,
    validate,
    transform,
    load,
    dashboard_report,
)

# ── Task wrapper functions ────────────────────────────────────────
# Each task receives the shared context dict and returns its result.
# Results are stored in context[task_id] for downstream tasks.

def task_generate(ctx):
    from src.generate_source_data import run
    ris, pacs, epr = run()
    return {"ris": ris, "pacs": pacs, "epr": epr}

def task_extract(ctx):
    from src.extract import run
    ris, pacs, epr = run()
    return {"ris": ris, "pacs": pacs, "epr": epr}

def task_validate(ctx):
    from src.validate import run
    upstream = ctx.get("task_extract", {})
    ris_v, pacs_v, epr_v, report = run(
        ris_df  = upstream.get("ris"),
        pacs_df = upstream.get("pacs"),
        epr_df  = upstream.get("epr"),
    )
    return {"ris": ris_v, "pacs": pacs_v, "epr": epr_v, "report": report}

def task_transform(ctx):
    from src.transform import run
    upstream = ctx.get("task_validate", {})
    fact, d_date, d_mod, d_rad, d_loc = run(
        ris_df  = upstream.get("ris"),
        pacs_df = upstream.get("pacs"),
        epr_df  = upstream.get("epr"),
    )
    return {"fact": fact, "dim_date": d_date,
            "dim_mod": d_mod, "dim_rad": d_rad, "dim_loc": d_loc}

def task_load(ctx):
    from src.load import run
    upstream = ctx.get("task_transform", {})
    result = run(
        fact_df         = upstream.get("fact"),
        dim_date        = upstream.get("dim_date"),
        dim_modality    = upstream.get("dim_mod"),
        dim_radiologist = upstream.get("dim_rad"),
        dim_location    = upstream.get("dim_loc"),
    )
    return result

def task_report(ctx):
    from src.dashboard_report import run
    return run(ctx)


# ── Build the DAG ─────────────────────────────────────────────────
def build_pipeline() -> PipelineOrchestrator:
    dag = PipelineOrchestrator("Radiology ETL – Daily Pipeline")

    dag.add_task(PipelineTask(
        task_id     = "task_generate",
        description = "Generate / extract source data (RIS, PACS, EPR)",
        func        = task_generate,
        depends_on  = [],
    ))
    dag.add_task(PipelineTask(
        task_id     = "task_extract",
        description = "Incremental extraction with watermark timestamps",
        func        = task_extract,
        depends_on  = ["task_generate"],
    ))
    dag.add_task(PipelineTask(
        task_id     = "task_validate",
        description = "Staging & validation (V01–V07 rules)",
        func        = task_validate,
        depends_on  = ["task_extract"],
    ))
    dag.add_task(PipelineTask(
        task_id     = "task_transform",
        description = "Transformation engine – star schema construction",
        func        = task_transform,
        depends_on  = ["task_validate"],
    ))
    dag.add_task(PipelineTask(
        task_id     = "task_load",
        description = "Incremental load to analytical warehouse",
        func        = task_load,
        depends_on  = ["task_transform"],
    ))
    dag.add_task(PipelineTask(
        task_id     = "task_report",
        description = "KPI dashboard report generation",
        func        = task_report,
        depends_on  = ["task_load"],
    ))

    return dag


# ── Main ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║     RADIOLOGY ETL PLATFORM – PROOF OF CONCEPT           ║")
    log.info("║     Multiverse Advanced Data Fellowship                  ║")
    log.info("╚══════════════════════════════════════════════════════════╝")
    log.info(f"  Run started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    pipeline = build_pipeline()
    success  = pipeline.run()

    if success:
        log.info("\n  ✓ Pipeline completed successfully.")
        log.info(f"  ✓ Warehouse: data/warehouse/radiology_warehouse.db")
        log.info(f"  ✓ Report:    reports/kpi_report.xlsx")
        log.info(f"  ✓ Audit log: logs/pipeline.log")
    else:
        log.error("\n  ✗ Pipeline completed with errors. Check logs/pipeline.log")
        sys.exit(1)
