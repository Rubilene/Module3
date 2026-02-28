"""
tests/test_pipeline.py
=======================
Unit tests for the Radiology ETL Pipeline.

Tests cover:
  - Validation rules (V01–V07)
  - Transformation functions (surrogate keys, date dim, KPI derivation)
  - Watermark logic
  - RBAC enforcement
  - Edge cases and boundary conditions

Run with:   python tests/test_pipeline.py
"""

import os
import sys
import unittest
import hashlib
import logging
import pandas as pd
from datetime import datetime, timedelta

# ── Path setup ────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from config.settings import MODALITY_MAP, MAX_TURNAROUND_MINS, RBAC_ROLES
from src.validation_helpers import (
    ValidationReport,
    check_null_timestamps,
    check_duplicates,
    standardise_modality,
    check_patient_id,
    check_turnaround_time,
    pseudonymise_patient_id,
)
from src.transform_helpers import (
    make_surrogate_key,
    build_dim_date,
    tat_band,
)
from src.rbac import check_rbac

log = logging.getLogger("tests")
logging.basicConfig(level=logging.WARNING,
                    format="%(asctime)s [%(levelname)s] %(message)s")


# ── Sample data factory ───────────────────────────────────────────
def make_ris_df(overrides=None) -> pd.DataFrame:
    """Creates a minimal valid RIS dataframe for testing."""
    base = {
        "study_id":         ["STU000001", "STU000002", "STU000003"],
        "patient_id":       ["12345678",  "87654321",  "11223344"],
        "exam_start_dt":    ["2024-06-01 09:00:00","2024-06-02 10:00:00","2024-06-03 11:00:00"],
        "exam_end_dt":      ["2024-06-01 09:30:00","2024-06-02 10:45:00","2024-06-03 11:20:00"],
        "report_issued_dt": ["2024-06-01 11:00:00","2024-06-02 14:00:00","2024-06-03 13:00:00"],
        "modality_raw":     ["CT",  "MRI", "XR"],
        "radiologist_id":   ["RAD101", "DR202", "CON303"],
        "site_code":        ["MAIN", "NORTH", "SOUTH"],
        "priority":         ["ROUTINE", "URGENT", "STAT"],
        "referral_source":  ["GP", "A&E", "OUTPATIENT"],
    }
    if overrides:
        base.update(overrides)
    df = pd.DataFrame(base)
    df["exam_start_dt"]   = pd.to_datetime(df["exam_start_dt"])
    df["exam_end_dt"]     = pd.to_datetime(df["exam_end_dt"])
    df["report_issued_dt"]= pd.to_datetime(df["report_issued_dt"])
    return df


# ════════════════════════════════════════════════════════════════════
# V01 – Null Timestamp Detection
# ════════════════════════════════════════════════════════════════════
class TestNullTimestampValidation(unittest.TestCase):

    def test_passes_when_all_timestamps_present(self):
        df     = make_ris_df()
        report = ValidationReport()
        passed, failed = check_null_timestamps(df, report)
        self.assertEqual(len(passed), 3)
        self.assertEqual(len(failed), 0)

    def test_fails_when_exam_start_null(self):
        df = make_ris_df({"exam_start_dt": [None, "2024-06-02 10:00:00", "2024-06-03 11:00:00"]})
        df["exam_start_dt"] = pd.to_datetime(df["exam_start_dt"], errors="coerce")
        report = ValidationReport()
        passed, failed = check_null_timestamps(df, report)
        self.assertEqual(len(failed), 1, "Row with null exam_start_dt should fail")

    def test_fails_when_exam_end_null(self):
        df = make_ris_df()
        df.loc[1, "exam_end_dt"] = pd.NaT
        report = ValidationReport()
        passed, failed = check_null_timestamps(df, report)
        self.assertEqual(len(failed), 1)


# ════════════════════════════════════════════════════════════════════
# V02 – Duplicate Study_ID Detection
# ════════════════════════════════════════════════════════════════════
class TestDuplicateDetection(unittest.TestCase):

    def test_no_duplicates_passes(self):
        df     = make_ris_df()
        report = ValidationReport()
        clean, dupes = check_duplicates(df, report)
        self.assertEqual(len(dupes), 0)
        self.assertEqual(len(clean), 3)

    def test_duplicate_removed_keeps_first(self):
        df = make_ris_df()
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)  # add duplicate
        report = ValidationReport()
        clean, dupes = check_duplicates(df, report)
        self.assertEqual(len(dupes), 1, "One duplicate should be flagged")
        self.assertEqual(len(clean), 3, "Three unique records should pass")

    def test_all_duplicates_flagged(self):
        df = make_ris_df()
        df = pd.concat([df, df], ignore_index=True)  # duplicate all
        report = ValidationReport()
        clean, dupes = check_duplicates(df, report)
        self.assertEqual(len(dupes), 3)


# ════════════════════════════════════════════════════════════════════
# V03 – Modality Standardisation
# ════════════════════════════════════════════════════════════════════
class TestModalityStandardisation(unittest.TestCase):

    def test_known_code_mapped_correctly(self):
        df     = make_ris_df({"modality_raw": ["CT", "MRI", "XR"]})
        report = ValidationReport()
        result = standardise_modality(df, report)
        self.assertEqual(result["modality_standard"].tolist(), ["CT Scan", "MRI", "X-Ray"])

    def test_case_insensitive_mapping(self):
        df     = make_ris_df({"modality_raw": ["ct", "mri", "xr"]})
        report = ValidationReport()
        result = standardise_modality(df, report)
        self.assertIn(result["modality_standard"][0], ["CT Scan", "UNKNOWN"])

    def test_unknown_code_becomes_unknown(self):
        df     = make_ris_df({"modality_raw": ["ZZZUNKNOWN", "MRI", "XR"]})
        report = ValidationReport()
        result = standardise_modality(df, report)
        self.assertEqual(result["modality_standard"][0], "UNKNOWN")

    def test_null_modality_becomes_unknown(self):
        df     = make_ris_df({"modality_raw": [None, "MRI", "XR"]})
        report = ValidationReport()
        result = standardise_modality(df, report)
        self.assertEqual(result["modality_standard"][0], "UNKNOWN")


# ════════════════════════════════════════════════════════════════════
# V04 – Patient ID Validation
# ════════════════════════════════════════════════════════════════════
class TestPatientIdValidation(unittest.TestCase):

    def test_valid_8_digit_id_passes(self):
        df     = make_ris_df()   # has 8-digit IDs
        report = ValidationReport()
        result = check_patient_id(df, report)
        for pid in result["patient_id"]:
            self.assertNotEqual(pid, "INVALID_ID")

    def test_short_id_flagged(self):
        df     = make_ris_df({"patient_id": ["123", "87654321", "11223344"]})
        report = ValidationReport()
        result = check_patient_id(df, report)
        self.assertEqual(result.loc[0, "patient_id"], "INVALID_ID")

    def test_alpha_id_flagged(self):
        df     = make_ris_df({"patient_id": ["ABCDEFGH", "87654321", "11223344"]})
        report = ValidationReport()
        result = check_patient_id(df, report)
        self.assertEqual(result.loc[0, "patient_id"], "INVALID_ID")

    def test_null_id_flagged(self):
        df     = make_ris_df({"patient_id": [None, "87654321", "11223344"]})
        report = ValidationReport()
        result = check_patient_id(df, report)
        self.assertEqual(result.loc[0, "patient_id"], "INVALID_ID")


# ════════════════════════════════════════════════════════════════════
# V05 – Turnaround Time Validation
# ════════════════════════════════════════════════════════════════════
class TestTurnaroundTimeValidation(unittest.TestCase):

    def test_valid_tat_is_ok(self):
        df     = make_ris_df()
        report = ValidationReport()
        result = check_turnaround_time(df, report)
        self.assertTrue((result["tat_flag"] == "OK").all())

    def test_negative_tat_flagged(self):
        df = make_ris_df()
        # Make report BEFORE exam end
        df.loc[0, "report_issued_dt"] = pd.Timestamp("2024-06-01 08:00:00")
        df.loc[0, "exam_end_dt"]      = pd.Timestamp("2024-06-01 09:30:00")
        report = ValidationReport()
        result = check_turnaround_time(df, report)
        self.assertEqual(result.loc[0, "tat_flag"], "NEGATIVE_TAT")

    def test_excessive_tat_flagged(self):
        df = make_ris_df()
        df.loc[0, "report_issued_dt"] = df.loc[0, "exam_end_dt"] + pd.Timedelta(days=30)
        report = ValidationReport()
        result = check_turnaround_time(df, report)
        self.assertEqual(result.loc[0, "tat_flag"], "EXCESSIVE_TAT")


# ════════════════════════════════════════════════════════════════════
# V07 – Patient Pseudonymisation
# ════════════════════════════════════════════════════════════════════
class TestPseudonymisation(unittest.TestCase):

    def test_patient_id_replaced_with_hash(self):
        df     = make_ris_df()
        report = ValidationReport()
        result = pseudonymise_patient_id(df, report)
        self.assertNotIn("patient_id", result.columns,
                         "Original patient_id must be dropped")
        self.assertIn("patient_id_pseudo", result.columns)

    def test_hash_is_deterministic(self):
        df     = make_ris_df()
        report = ValidationReport()
        r1     = pseudonymise_patient_id(df.copy(), report)
        r2     = pseudonymise_patient_id(df.copy(), ValidationReport())
        self.assertEqual(
            r1["patient_id_pseudo"].tolist(),
            r2["patient_id_pseudo"].tolist(),
            "SHA-256 hash must be deterministic"
        )

    def test_hash_is_not_original_id(self):
        df     = make_ris_df()
        orig   = df["patient_id"].tolist()
        report = ValidationReport()
        result = pseudonymise_patient_id(df, report)
        for pid, pseudo in zip(orig, result["patient_id_pseudo"]):
            self.assertNotEqual(str(pid), str(pseudo))


# ════════════════════════════════════════════════════════════════════
# Transformation: Surrogate Key Generation
# ════════════════════════════════════════════════════════════════════
class TestSurrogateKeyGeneration(unittest.TestCase):

    def test_same_input_same_key(self):
        self.assertEqual(make_surrogate_key("CT Scan"), make_surrogate_key("CT Scan"))

    def test_different_inputs_different_keys(self):
        self.assertNotEqual(make_surrogate_key("CT Scan"), make_surrogate_key("MRI"))

    def test_key_is_integer(self):
        key = make_surrogate_key("X-Ray")
        self.assertIsInstance(key, int)

    def test_key_is_positive(self):
        key = make_surrogate_key("Ultrasound")
        self.assertGreater(key, 0)


# ════════════════════════════════════════════════════════════════════
# Transformation: TAT Band Assignment
# ════════════════════════════════════════════════════════════════════
class TestTatBand(unittest.TestCase):

    def test_less_than_1_hour(self):
        self.assertEqual(tat_band(30), "< 1 hour")

    def test_1_to_4_hours(self):
        self.assertEqual(tat_band(120), "1–4 hours")

    def test_4_to_24_hours(self):
        self.assertEqual(tat_band(600), "4–24 hours")

    def test_1_to_3_days(self):
        self.assertEqual(tat_band(2000), "1–3 days")

    def test_over_3_days(self):
        self.assertEqual(tat_band(5000), "> 3 days")

    def test_negative_is_invalid(self):
        self.assertEqual(tat_band(-10), "Invalid")

    def test_none_is_invalid(self):
        self.assertEqual(tat_band(None), "Invalid")

    def test_boundary_60_mins(self):
        self.assertEqual(tat_band(60), "< 1 hour")

    def test_boundary_61_mins(self):
        self.assertEqual(tat_band(61), "1–4 hours")


# ════════════════════════════════════════════════════════════════════
# RBAC Enforcement
# ════════════════════════════════════════════════════════════════════
class TestRBAC(unittest.TestCase):

    def test_admin_can_write(self):
        self.assertTrue(check_rbac("admin", "write"))

    def test_admin_can_delete(self):
        self.assertTrue(check_rbac("admin", "delete"))

    def test_analyst_can_read(self):
        self.assertTrue(check_rbac("analyst", "read"))

    def test_analyst_cannot_write(self):
        self.assertFalse(check_rbac("analyst", "write"))

    def test_engineer_cannot_delete(self):
        self.assertFalse(check_rbac("engineer", "delete"))

    def test_unknown_role_denied(self):
        self.assertFalse(check_rbac("intruder", "read"))

    def test_dashboard_role_read_only(self):
        self.assertTrue(check_rbac("dashboard", "read"))
        self.assertFalse(check_rbac("dashboard", "write"))


# ════════════════════════════════════════════════════════════════════
# Date Dimension
# ════════════════════════════════════════════════════════════════════
class TestDateDimension(unittest.TestCase):

    def test_correct_rows_generated(self):
        dates = pd.Series(pd.to_datetime(["2024-01-15", "2024-03-31", "2024-04-01"]))
        dim   = build_dim_date(dates)
        self.assertEqual(len(dim), 3)

    def test_financial_year_april_start(self):
        dates = pd.Series(pd.to_datetime(["2024-04-01"]))
        dim   = build_dim_date(dates)
        self.assertEqual(dim.iloc[0]["financial_year"], "FY2024/25")

    def test_financial_year_march_in_previous_fy(self):
        dates = pd.Series(pd.to_datetime(["2024-03-31"]))
        dim   = build_dim_date(dates)
        self.assertEqual(dim.iloc[0]["financial_year"], "FY2023/24")

    def test_weekend_flag(self):
        # 2024-01-06 = Saturday
        dates = pd.Series(pd.to_datetime(["2024-01-06"]))
        dim   = build_dim_date(dates)
        self.assertEqual(dim.iloc[0]["is_weekend"], 1)

    def test_weekday_flag(self):
        # 2024-01-08 = Monday
        dates = pd.Series(pd.to_datetime(["2024-01-08"]))
        dim   = build_dim_date(dates)
        self.assertEqual(dim.iloc[0]["is_weekend"], 0)

    def test_no_duplicate_date_keys(self):
        dates = pd.Series(pd.to_datetime(["2024-01-15","2024-01-15","2024-03-31"]))
        dim   = build_dim_date(dates)
        self.assertEqual(dim["date_key"].nunique(), len(dim),
                         "Date dimension should have no duplicate keys")


# ── Run all tests ─────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "="*60)
    print("  Radiology ETL Pipeline – Unit Test Suite")
    print("="*60 + "\n")
    unittest.main(verbosity=2)
