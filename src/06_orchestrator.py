"""
src/06_orchestrator.py
=======================
Pipeline Orchestration – simulates Apache Airflow DAG behaviour.

In production this would be an Airflow DAG with:
  - Scheduled trigger at 05:00 daily
  - Task dependencies (extract → validate → transform → load → report)
  - Retry logic with exponential backoff
  - SLA breach alerting
  - Dead-letter queue for failed records

Here we implement equivalent logic using Python to demonstrate
the orchestration design pattern described in the PoC.
"""

import os
import sys
import time
import logging
import traceback
from datetime import datetime
from dataclasses import dataclass, field
from typing import Callable, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MAX_RETRIES, RETRY_DELAY_SECS, LOG_FILE, LOGS_DIR

log = logging.getLogger("etl.orchestrator")

# ── Task status ───────────────────────────────────────────────────
class TaskStatus:
    PENDING  = "PENDING"
    RUNNING  = "RUNNING"
    SUCCESS  = "SUCCESS"
    FAILED   = "FAILED"
    SKIPPED  = "SKIPPED"
    RETRYING = "RETRYING"

# ── Task definition ───────────────────────────────────────────────
@dataclass
class PipelineTask:
    task_id:      str
    description:  str
    func:         Callable
    depends_on:   List[str] = field(default_factory=list)
    max_retries:  int       = MAX_RETRIES
    retry_delay:  float     = RETRY_DELAY_SECS
    status:       str       = TaskStatus.PENDING
    start_time:   Optional[datetime] = None
    end_time:     Optional[datetime] = None
    result:       object    = None
    error:        str       = ""
    attempts:     int       = 0

    @property
    def duration_secs(self):
        if self.start_time and self.end_time:
            return round((self.end_time - self.start_time).total_seconds(), 2)
        return None

# ── Orchestrator ──────────────────────────────────────────────────
class PipelineOrchestrator:
    """
    Directed Acyclic Graph (DAG) pipeline runner.
    Executes tasks in dependency order with retry logic.
    """

    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.tasks: dict[str, PipelineTask] = {}
        self.run_id = datetime.now().strftime("RUN_%Y%m%d_%H%M%S")
        self.context = {}   # shared state passed between tasks

    def add_task(self, task: PipelineTask):
        self.tasks[task.task_id] = task

    def _resolve_order(self) -> List[str]:
        """Topological sort – resolve task execution order from dependencies."""
        completed = set()
        order = []
        remaining = list(self.tasks.keys())

        max_iterations = len(remaining) * 2
        iteration = 0
        while remaining:
            iteration += 1
            if iteration > max_iterations:
                raise RuntimeError(f"Circular dependency detected in pipeline: {remaining}")
            for tid in list(remaining):
                task = self.tasks[tid]
                if all(dep in completed for dep in task.depends_on):
                    order.append(tid)
                    completed.add(tid)
                    remaining.remove(tid)
        return order

    def _run_task_with_retry(self, task: PipelineTask) -> bool:
        """Execute a task with exponential backoff retry on failure."""
        for attempt in range(1, task.max_retries + 1):
            task.attempts = attempt
            try:
                log.info(f"  ▶ [{task.task_id}] {task.description}"
                         + (f" (attempt {attempt}/{task.max_retries})" if attempt > 1 else ""))
                task.status     = TaskStatus.RUNNING
                task.start_time = datetime.now()

                # Run the task function, passing shared context
                result       = task.func(self.context)
                task.result  = result
                task.end_time = datetime.now()
                task.status   = TaskStatus.SUCCESS

                # Update shared context with result for downstream tasks
                self.context[task.task_id] = result
                log.info(f"  ✓ [{task.task_id}] Complete in {task.duration_secs}s")
                return True

            except Exception as e:
                task.error    = str(e)
                task.end_time = datetime.now()
                log.error(f"  ✗ [{task.task_id}] Attempt {attempt} failed: {e}")
                log.debug(traceback.format_exc())

                if attempt < task.max_retries:
                    delay = task.retry_delay * (2 ** (attempt - 1))   # exponential backoff
                    task.status = TaskStatus.RETRYING
                    log.info(f"  ↻ [{task.task_id}] Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    task.status = TaskStatus.FAILED
                    log.error(f"  ✗ [{task.task_id}] All {task.max_retries} attempts failed.")
                    return False
        return False

    def run(self) -> bool:
        """Execute the full pipeline in dependency order."""
        log.info("=" * 60)
        log.info(f"PIPELINE: {self.pipeline_name}")
        log.info(f"RUN ID:   {self.run_id}")
        log.info(f"START:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info("=" * 60)

        pipeline_start = datetime.now()
        order = self._resolve_order()
        log.info(f"  Execution order: {' → '.join(order)}\n")

        success = True
        for tid in order:
            task = self.tasks[tid]

            # Check dependencies succeeded
            failed_deps = [d for d in task.depends_on
                           if self.tasks[d].status != TaskStatus.SUCCESS]
            if failed_deps:
                task.status = TaskStatus.SKIPPED
                log.warning(f"  ⊘ [{tid}] SKIPPED – upstream failures: {failed_deps}")
                success = False
                continue

            task_ok = self._run_task_with_retry(task)
            if not task_ok:
                success = False

        self._print_run_summary(pipeline_start)
        return success

    def _print_run_summary(self, pipeline_start: datetime):
        total_secs = round((datetime.now() - pipeline_start).total_seconds(), 1)
        log.info("\n" + "=" * 60)
        log.info(f"PIPELINE RUN SUMMARY  |  {self.run_id}")
        log.info("=" * 60)
        for tid in self._resolve_order():
            t = self.tasks[tid]
            icon = {"SUCCESS":"✓","FAILED":"✗","SKIPPED":"⊘","PENDING":"○"}.get(t.status,"?")
            dur  = f"{t.duration_secs}s" if t.duration_secs else "—"
            log.info(f"  {icon} {tid:<30} {t.status:<10} {dur:>8}"
                     + (f"  [attempts: {t.attempts}]" if t.attempts > 1 else ""))
        log.info(f"\n  Total pipeline duration: {total_secs}s")

        all_ok  = all(t.status == TaskStatus.SUCCESS for t in self.tasks.values())
        outcome = "✓ PIPELINE SUCCEEDED" if all_ok else "✗ PIPELINE COMPLETED WITH ERRORS"
        log.info(f"  {outcome}")
        log.info("=" * 60)


# ── Schedule simulation ───────────────────────────────────────────
def simulate_daily_schedule():
    """
    Simulates the Apache Airflow 05:00 daily trigger.
    In production this would be an Airflow cron schedule:
        schedule_interval = '0 5 * * *'
    """
    log.info("  Simulating daily scheduled trigger (05:00)")
    log.info("  [In production: Apache Airflow cron '0 5 * * *']")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    simulate_daily_schedule()
    log.info("Orchestrator module loaded – use run_pipeline.py to execute.")
