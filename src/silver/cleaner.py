"""
Silver Layer Cleaner.

Applies cleaning rules from config to standardize data using Polars
vectorized column operations for high-throughput batch processing.
"""

from typing import Dict, Any
from datetime import datetime
from dateutil import parser as date_parser

import polars as pl
from loguru import logger


class SilverCleaner:
    """
    Applies cleaning rules to standardize data via Polars vectorized expressions.

    Supports:
    - Date normalization (ISO format)
    - Boolean normalization
    - Case normalization (lower / upper / title)
    - Phone normalization (strip formatting characters)
    - String trimming / whitespace normalization
    """

    def __init__(self, cleaning_rules: Dict[str, Any]):
        """
        Initialize cleaner with rules from config.

        Args:
            cleaning_rules: Cleaning rules from cleaning_rules.yaml
        """
        self.rules = cleaning_rules.get("cleaners", {})
        self.logger = logger.bind(component="SilverCleaner")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def clean_column(
        self,
        df: pl.DataFrame,
        field_name: str,
        rule_name: str,
    ) -> tuple[pl.DataFrame, bool]:
        """
        Apply a cleaning rule to an entire DataFrame column using Polars
        vectorized expressions.

        Args:
            df: Input DataFrame.
            field_name: Column to clean.
            rule_name: Key in ``self.rules`` (e.g. "lowercase", "date_iso").

        Returns:
            Tuple of (transformed DataFrame, whether the column was changed).
        """
        rule = self.rules.get(rule_name)
        if not rule:
            self.logger.warning(f"Unknown cleaning rule '{rule_name}' for {field_name}")
            return df, False

        rule_type = rule.get("type")

        try:
            if rule_type == "case":
                return self._clean_column_case(df, field_name, rule)
            elif rule_type == "phone":
                return self._clean_column_phone(df, field_name, rule)
            elif rule_type == "boolean":
                return self._clean_column_boolean(df, field_name, rule)
            elif rule_type == "date":
                return self._clean_column_date(df, field_name, rule)
            elif rule_type == "string":
                return self._clean_column_string(df, field_name, rule)
            else:
                self.logger.warning(
                    f"Unsupported rule type '{rule_type}' for {field_name}"
                )
                return df, False
        except Exception as e:
            self.logger.warning(
                f"Cleaning column {field_name} with {rule_name} failed: {e}"
            )
            return df, False

    # ------------------------------------------------------------------
    # Internal helpers (one per rule type)
    # ------------------------------------------------------------------

    def _clean_column_case(
        self, df: pl.DataFrame, col: str, rule: Dict[str, Any],
    ) -> tuple[pl.DataFrame, bool]:
        if df[col].dtype not in (pl.String, pl.Utf8):
            return df, False
        case = rule.get("case", "lower")
        if case == "lower":
            df = df.with_columns(pl.col(col).str.to_lowercase().alias(col))
        elif case == "upper":
            df = df.with_columns(pl.col(col).str.to_uppercase().alias(col))
        elif case == "title":
            df = df.with_columns(pl.col(col).str.to_titlecase().alias(col))
        else:
            return df, False
        return df, True

    def _clean_column_phone(
        self, df: pl.DataFrame, col: str, rule: Dict[str, Any],
    ) -> tuple[pl.DataFrame, bool]:
        if df[col].dtype not in (pl.String, pl.Utf8):
            return df, False
        df = df.with_columns(
            pl.col(col)
            .str.replace_all(r"[\(\)\-\s\.+]", "")
            .alias(col)
        )
        return df, True

    def _clean_column_boolean(
        self, df: pl.DataFrame, col: str, rule: Dict[str, Any],
    ) -> tuple[pl.DataFrame, bool]:
        if df[col].dtype not in (pl.String, pl.Utf8):
            return df, False
        df = df.with_columns(
            pl.when(
                pl.col(col).str.to_lowercase().is_in(
                    ["true", "yes", "1", "y"]
                )
            )
            .then(pl.lit(True))
            .when(
                pl.col(col).str.to_lowercase().is_in(
                    ["false", "no", "0", "n"]
                )
            )
            .then(pl.lit(False))
            .otherwise(pl.lit(None))
            .alias(col)
        )
        return df, True

    def _clean_column_date(
        self, df: pl.DataFrame, col: str, rule: Dict[str, Any],
    ) -> tuple[pl.DataFrame, bool]:
        if df[col].dtype not in (pl.String, pl.Utf8):
            return df, False

        def _normalize_date(val):
            if val is None or val == "":
                return val
            try:
                if str(val).replace(".", "", 1).isdigit():
                    ts = float(val)
                    if 0 <= ts <= 4102444800:
                        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass
            try:
                parsed = date_parser.parse(str(val), dayfirst=False, fuzzy=False)
                return parsed.strftime("%Y-%m-%d")
            except (ValueError, TypeError, OverflowError):
                return val

        df = df.with_columns(
            pl.col(col)
            .map_elements(_normalize_date, return_dtype=pl.String)
            .alias(col)
        )
        return df, True

    def _clean_column_string(
        self, df: pl.DataFrame, col: str, rule: Dict[str, Any],
    ) -> tuple[pl.DataFrame, bool]:
        if df[col].dtype not in (pl.String, pl.Utf8):
            return df, False
        operations = rule.get("operations", [])
        changed = False
        if "trim" in operations:
            df = df.with_columns(pl.col(col).str.strip_chars().alias(col))
            changed = True
        if "normalize_whitespace" in operations:
            df = df.with_columns(
                pl.col(col).str.replace_all(r"\s+", " ").alias(col)
            )
            changed = True
        return df, changed
