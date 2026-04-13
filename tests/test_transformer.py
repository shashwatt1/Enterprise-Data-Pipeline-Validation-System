"""
Tests for the data transformation service.
"""

import pandas as pd
import pytest

from app.services.transformer import (
    _strip_whitespace,
    _lowercase,
    _uppercase,
    _title_case,
    _to_integer,
    _parse_date,
    _parse_currency,
    _standardize_phone,
)


class TestTransformActions:
    def test_strip_whitespace(self):
        s = pd.Series(["  hello  ", " world ", "test"])
        result = _strip_whitespace(s, {})
        assert list(result) == ["hello", "world", "test"]

    def test_lowercase(self):
        s = pd.Series(["HELLO", "World", "TEST"])
        result = _lowercase(s, {})
        assert list(result) == ["hello", "world", "test"]

    def test_uppercase(self):
        s = pd.Series(["hello", "World", "test"])
        result = _uppercase(s, {})
        assert list(result) == ["HELLO", "WORLD", "TEST"]

    def test_title_case(self):
        s = pd.Series(["new york", "LOS ANGELES", "san francisco"])
        result = _title_case(s, {})
        assert list(result) == ["New York", "Los Angeles", "San Francisco"]

    def test_to_integer(self):
        s = pd.Series(["1", "2", "abc", "4"])
        result = _to_integer(s, {})
        assert result.iloc[0] == 1.0
        assert result.iloc[1] == 2.0
        assert pd.isna(result.iloc[2])
        assert result.iloc[3] == 4.0

    def test_parse_date_multiple_formats(self):
        s = pd.Series(["2023-01-15", "01/20/2023", "15-Mar-2023", "2023/04/01"])
        result = _parse_date(s, {"format": "auto"})
        assert result.iloc[0].day == 15
        assert result.iloc[1].month == 1
        assert result.iloc[2].month == 3
        assert result.iloc[3].month == 4

    def test_parse_date_empty(self):
        s = pd.Series(["", None])
        result = _parse_date(s, {"format": "auto"})
        assert result.iloc[0] is None

    def test_parse_currency(self):
        s = pd.Series(["$150.00", "$1,200.50", "$-25.00", "$0.00"])
        result = _parse_currency(s, {"strip_chars": "$,"})
        assert result.iloc[0] == 150.00
        assert result.iloc[1] == 1200.50
        assert result.iloc[2] == -25.00
        assert result.iloc[3] == 0.00

    def test_standardize_phone(self):
        s = pd.Series(["+1-555-0101", "5550102", "1-555-0103", ""])
        result = _standardize_phone(s, {})
        # +1-555-0101 → digits: 15550101 (8 digits) → returned as-is
        assert result.iloc[0] == "15550101"
        # 5550102 → 7 digits → returned as-is
        assert result.iloc[1] == "5550102"
        # 1-555-0103 → digits: 15550103 (8 digits) → returned as-is
        assert result.iloc[2] == "15550103"
        assert result.iloc[3] is None

    def test_parse_currency_empty(self):
        s = pd.Series(["", None])
        result = _parse_currency(s, {"strip_chars": "$,"})
        assert result.iloc[0] is None
