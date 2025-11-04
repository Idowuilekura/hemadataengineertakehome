from __future__ import annotations

"""
Utilities for the Silver layer of the ETL pipeline.

This module handles:
- Cleaning and standardizing columns from the Bronze layer.
- Casting and validating date and numeric fields.
- Detecting and isolating bad or malformed records.
- Enriching datasets with order, shipping, customer, and location features.
- Generating clean and consistent schemas for Silver outputs.

All functions include logging and error handling to ensure reliable, traceable data transformation
before promotion to the Gold layer.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

import pyspark
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import types as T

from utils.general_utils import get_logger, build_spark

LOG_LEVEL = "INFO"
log = get_logger(level=LOG_LEVEL)


def non_empty(c: str):
    """Return a boolean expression indicating if a column is non-empty (not null or blank)."""
    return F.col(c).isNotNull() & (F.trim(F.col(c)).cast("string") != "")


def cast_and_collect_bad(df: DataFrame,
                         date_cols: List[str],
                         float_cols: List[str],
                         date_fmt: str = "dd/MM/yyyy",
                         dedup_bad: bool = True) -> Tuple[DataFrame, DataFrame]:
    """Cast date and numeric columns while collecting rows that fail type conversions.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame.
    date_cols : list[str]
        Column names containing date-like values to cast using `to_timestamp` with `date_fmt`.
    float_cols : list[str]
        Column names to cast to float using `try_cast`.
    date_fmt : str, optional
        Source date format string, by default "dd/MM/yyyy".
    dedup_bad : bool, optional
        If True, drop duplicate rows in the collected bad set, by default True.

    Returns
    -------
    tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]
        A tuple of (good_df, bad_df) where:
        - good_df has the casts applied and duplicates removed,
        - bad_df contains rows where the cast failed for any targeted column.
    """
    try:
        df2: DataFrame = df
        bad_parts: List[DataFrame] = []
        for c in date_cols:
            if c in df2.columns:
                cast_col = F.to_timestamp(F.col(c), date_fmt)
                bad_c = df2.filter(cast_col.isNull() & non_empty(c)).select(*df.columns)
                bad_parts.append(bad_c)
                df2 = df2.withColumn(c, cast_col)
        for c in float_cols:
            if c in df2.columns:
                cast_col = F.expr(f"try_cast(`{c}` as float)")
                bad_c = df2.filter(cast_col.isNull() & non_empty(c)).select(*df.columns)
                bad_parts.append(bad_c)
                df2 = df2.withColumn(c, cast_col)
        if bad_parts:
            bad_df = bad_parts[0]
            for part in bad_parts[1:]:
                bad_df = bad_df.unionByName(part, allowMissingColumns=True)
            if dedup_bad:
                bad_df = bad_df.dropDuplicates()
        else:
            bad_df = df2.limit(0).select(*df.columns)
        good_df = df2.dropDuplicates()
        log.debug("cast_and_collect_bad produced good_df=%s rows, bad_df=%s rows",
                  good_df.count(), bad_df.count())
        return good_df, bad_df
    except Exception as exc:
        log.exception("cast_and_collect_bad failed: %s", exc)
        raise


def add_sales_features(
    df: DataFrame,
    order_col: str = "order_date",
    ship_col: str = "ship_date",
    ship_mode_col: str = "ship_mode",
    customer_name_col: str = "customer_name",
    country_col: str = "country",
    city_col: str = "city",
    date_fmt: str = "dd/MM/yyyy",
) -> DataFrame:
    """Add derived order, shipping, customer, and location features to a DataFrame.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame.
    order_col : str, optional
        Order date column name, by default "order_date".
    ship_col : str, optional
        Ship date column name, by default "ship_date".
    ship_mode_col : str, optional
        Ship mode column name, by default "ship_mode".
    customer_name_col : str, optional
        Customer name column, by default "customer_name".
    country_col : str, optional
        Country column, by default "country".
    city_col : str, optional
        City column, by default "city".
    date_fmt : str, optional
        Date format for parsing, by default "dd/MM/yyyy".

    Returns
    -------
    pyspark.sql.DataFrame
        Enriched DataFrame with added features and standardized date columns.
    """
    try:
        parsed = df.withColumns({
            "order_date_ts": F.to_timestamp(F.col(order_col), date_fmt),
            "ship_date_ts":  F.to_timestamp(F.col(ship_col),  date_fmt),
        })
        order_feats = parsed.withColumns({
            "order_year":            F.year("order_date_ts"),
            "order_quarter":         F.quarter("order_date_ts"),
            "order_month":           F.month("order_date_ts"),
            "order_day":             F.dayofmonth("order_date_ts"),
            "order_weekofyear":      F.weekofyear("order_date_ts"),
            "order_day_of_week_num": F.dayofweek("order_date_ts"),
            "order_day_of_week":     F.date_format("order_date_ts", "EEEE"),
            "order_is_workday":      ~F.dayofweek("order_date_ts").isin(1, 7),
            "order_is_month_start":  (F.dayofmonth("order_date_ts") == F.lit(1)),
            "order_is_month_end":    (F.dayofmonth("order_date_ts") == F.dayofmonth(F.last_day("order_date_ts"))),
            "order_yyyymm":          F.date_format("order_date_ts", "yyyy-MM"),
        })
        ship_feats = order_feats.withColumns({
            "ship_year":            F.year("ship_date_ts"),
            "ship_quarter":         F.quarter("ship_date_ts"),
            "ship_month":           F.month("ship_date_ts"),
            "ship_day":             F.dayofmonth("ship_date_ts"),
            "ship_weekofyear":      F.weekofyear("ship_date_ts"),
            "ship_day_of_week_num": F.dayofweek("ship_date_ts"),
            "ship_day_of_week":     F.date_format("ship_date_ts", "EEEE"),
            "ship_is_workday":      ~F.dayofweek("ship_date_ts").isin(1, 7),
            "ship_is_month_start":  (F.dayofmonth("ship_date_ts") == F.lit(1)),
            "ship_is_month_end":    (F.dayofmonth("ship_date_ts") == F.dayofmonth(F.last_day("ship_date_ts"))),
            "ship_yyyymm":          F.date_format("ship_date_ts", "yyyy-MM"),
        })
        cross = ship_feats.withColumns({
            "days_to_ship":         F.datediff("ship_date_ts", "order_date_ts"),
            "same_day_shipping":    (F.datediff("ship_date_ts", "order_date_ts") == F.lit(0)),
            "is_negative_leadtime": (F.datediff("ship_date_ts", "order_date_ts") < 0),
            "ship_speed_bucket": (
                F.when(F.col("days_to_ship") <= 1, "express")
                 .when(F.col("days_to_ship") <= 3, "standard")
                 .when(F.col("days_to_ship") >  3, "slow")
                 .otherwise(None)
            ),
        })
        out = cross
        if ship_mode_col in out.columns:
            out = out.withColumn("ship_mode_norm", F.lower(F.trim(F.col(ship_mode_col))))
        if customer_name_col in out.columns:
            parts = F.split(F.col(customer_name_col), r"\s+")
            out = out.withColumns({
                "customer_first_name": F.element_at(parts, 1),
                "customer_last_name":  F.when(F.size(parts) > 1, F.element_at(parts, -1)),
                "customer_name_len":   F.length(F.col(customer_name_col)),
            })
        if city_col in out.columns and country_col in out.columns:
            out = out.withColumn("city_country", F.concat_ws(", ", F.col(city_col), F.col(country_col)))
        to_drop = [c for c in [order_col, ship_col] if c in out.columns]
        out = out.drop(*to_drop)
        rename_map = {k: v for k, v in {
            "order_date_ts": "order_date",
            "ship_date_ts":  "shipment_date",
        }.items() if k in out.columns}
        out = out.withColumnsRenamed(rename_map)
        if "ship_mode" in out.columns:
            out = out.withColumnRenamed("ship_mode", "shipment_mode")
        if "segment" in out.columns:
            out = out.withColumnRenamed("segment", "customer_segment")
        log.debug("add_sales_features completed.")
        return out
    except Exception as exc:
        log.exception("add_sales_features failed: %s", exc)
        raise


def get_df_schema(df: pyspark.sql.DataFrame, columns_to_drop: List[str]) -> T.StructType:
    """Return the schema of a DataFrame after dropping specified columns.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame.
    columns_to_drop : list[str]
        Column names to drop prior to computing the schema.

    Returns
    -------
    pyspark.sql.types.StructType
        The resulting schema.
    """
    try:
        df2 = df.drop(*columns_to_drop)
        return df2.schema
    except Exception as exc:
        log.exception("get_df_schema failed: %s", exc)
        raise


def schema_json_to_struct(schema_json: str) -> T.StructType:
    """Convert a JSON-encoded Spark schema string into a StructType.

    Parameters
    ----------
    schema_json : str
        JSON string (as produced by `StructType.json()` or similar).

    Returns
    -------
    pyspark.sql.types.StructType
        Parsed Spark StructType.
    """
    try:
        schema_obj = json.loads(schema_json)
        return T.StructType.fromJson(schema_obj)
    except Exception as exc:
        log.exception("schema_json_to_struct failed: %s", exc)
        raise


def silver_column_clean_enrich_schema(df: DataFrame,
                                      date_cols: List[str],
                                      float_cols: List[str],
                                      date_fmt: str = "dd/MM/yyyy",
                                      dedup_bad: bool = True
                                      ) -> Tuple[DataFrame, DataFrame, T.StructType]:
    """Validate/cast date & numeric columns, enrich with sales features, and return final schema.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame.
    date_cols : list[str]
        Date-like columns to validate and cast.
    float_cols : list[str]
        Numeric-like columns to validate and cast.
    date_fmt : str, optional
        Date format for casting, by default "dd/MM/yyyy".
    dedup_bad : bool, optional
        Drop duplicates in bad records, by default True.

    Returns
    -------
    tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame, pyspark.sql.types.StructType]
        (enriched_good_df, bad_df, silver_schema)
    """
    good_df, bad_df = cast_and_collect_bad(df, date_cols=date_cols, float_cols=float_cols, date_fmt="dd/MM/yyyy", dedup_bad=True)
    df = add_sales_features(good_df)
    silver_schema = get_df_schema(df, columns_to_drop=["row_id"])
    return df, bad_df, silver_schema