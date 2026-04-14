"""
Tests for the data validation service.
"""

import pandas as pd
import pytest

from app.services.validator import (
    _check_not_null,
    _check_unique,
    _check_range,
    _check_date_range,
    _check_pattern,
)


class TestValidationChecks:
    def test_null_check_pass(self):
        df = pd.DataFrame({"col": [1, 2, 3]})
        result = _check_not_null(df, "col", {}, "col_not_null")
        assert result["status"] == "PASS"
        assert result["affected_rows"] == 0

    def test_null_check_fail(self):
        df = pd.DataFrame({"col": [1, None, 3, None]})
        result = _check_not_null(df, "col", {}, "col_not_null")
        assert result["status"] == "FAIL"
        assert result["affected_rows"] == 2

    def test_unique_check_pass(self):
        df = pd.DataFrame({"col": [1, 2, 3, 4]})
        result = _check_unique(df, "col", {}, "col_unique")
        assert result["status"] == "PASS"

    def test_unique_check_fail(self):
        df = pd.DataFrame({"col": [1, 2, 2, 3]})
        result = _check_unique(df, "col", {}, "col_unique")
        assert result["status"] == "FAIL"
        assert result["affected_rows"] == 1

    def test_range_check_pass(self):
        df = pd.DataFrame({"col": [5, 10, 15]})
        result = _check_range(df, "col", {"min": 1, "max": 20}, "col_range")
        assert result["status"] == "PASS"

    def test_range_check_fail(self):
        df = pd.DataFrame({"col": [5, 25, -1, 10]})
        result = _check_range(df, "col", {"min": 0, "max": 20}, "col_range")
        assert result["status"] == "FAIL"
        assert result["affected_rows"] == 2  # 25 and -1

    def test_date_range_pass(self):
        df = pd.DataFrame({"col": pd.to_datetime(["2023-06-01", "2024-01-01"])})
        result = _check_date_range(
            df, "col", {"min": "2023-01-01", "max": "2025-01-01"}, "col_date"
        )
        assert result["status"] == "PASS"

    def test_date_range_fail(self):
        df = pd.DataFrame({
            "col": pd.to_datetime(["2019-01-01", "2023-06-01", "2030-01-01"])
        })
        result = _check_date_range(
            df, "col", {"min": "2020-01-01", "max": "2026-12-31"}, "col_date"
        )
        assert result["status"] == "FAIL"
        assert result["affected_rows"] == 2  # 2019 and 2030

    def test_pattern_pass(self):
        df = pd.DataFrame({"col": ["user@mail.com", "test@test.org"]})
        result = _check_pattern(
            df, "col",
            {"regex": r"^[\w.+-]+@[\w-]+\.[\w.]+$"},
            "col_pattern",
        )
        assert result["status"] == "PASS"

    def test_pattern_fail(self):
        df = pd.DataFrame({"col": ["valid@mail.com", "notanemail", "also@bad"]})
        result = _check_pattern(
            df, "col",
            {"regex": r"^[\w.+-]+@[\w-]+\.\w{2,}$"},
            "col_pattern",
        )
        assert result["status"] == "FAIL"
        assert result["affected_rows"] >= 1
