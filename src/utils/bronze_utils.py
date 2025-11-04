from __future__ import annotations

import json
import logging
import os
from copy import deepcopy
from typing import Any, Dict, Optional, Tuple
import datetime

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from utils.general_utils import LOGGER

# LOG_LEVEL = "INFO"


# class SeparatorFormatter(logging.Formatter):
#     """Custom formatter that:
#     - Appends a separator line after every log record.
#     - Includes the function name for ERROR and higher logs.
#     """

#     SEP = "-" * 30

#     def format(self, record: logging.LogRecord) -> str:
#         if record.levelno >= logging.ERROR:
#             fmt = "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"
#         else:
#             fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
#         base = logging.Formatter(fmt)
#         rendered = base.format(record)
#         return f"{rendered}\n{self.SEP}"


# def get_logger(name: str = "hema_etl", level: str = LOG_LEVEL) -> logging.Logger:
#     """Create (or retrieve) a configured logger.

#     Parameters
#     ----------
#     name : str, optional
#         Logger name; use module or application name to differentiate streams.
#     level : str, optional
#         Logging level as a string (e.g., "INFO", "DEBUG", "WARNING").

#     Returns
#     -------
#     logging.Logger
#         Configured logger instance.
#     """
#     logger = logging.getLogger(name)
#     if not logger.handlers:
#         handler = logging.StreamHandler()
#         handler.setFormatter(SeparatorFormatter())
#         logger.addHandler(handler)
#     logger.setLevel(level.upper())
#     return logger


# LOGGER = get_logger(__name__, LOG_LEVEL)


# def build_spark(app_name: str = "hema_etl_notebook",
#                 remote_url: Optional[str] = None,
#                 local: bool = True) -> SparkSession:
#     """Create a SparkSession either locally or via remote mode."""
#     try:
#         if local:
#             LOGGER.info("Creating local SparkSession with app_name=%s", app_name)
#             return SparkSession.builder.appName(app_name).getOrCreate()
#         else:
#             if not remote_url:
#                 raise ValueError("remote_url must be provided when local=False")
#             LOGGER.info("Connecting to remote SparkSession at %s (app_name=%s)", remote_url, app_name)
#             return SparkSession.builder.remote(remote_url).appName(app_name).getOrCreate()
#     except Exception as exc:
#         LOGGER.exception("Failed to create SparkSession: %s", exc)
#         raise


def get_file_info(file_path: str) -> Dict[str, Dict[str, str]]:
    """Return last modified time (ISO string) and size (bytes) for a file."""
    if not os.path.isfile(file_path):
        LOGGER.error("File not found at path: %s", file_path)
        raise FileNotFoundError(f"File not found: {file_path}")

    modified_time = os.path.getmtime(file_path)
    file_size = str(os.path.getsize(file_path))
    modified_datetime = datetime.datetime.fromtimestamp(modified_time).isoformat()

    return {
        file_path: {
            "modified": modified_datetime,
            "size_bytes": file_size,
        }
    }


def check_metadata_in_folder(metadata_folder_path: str, metadata_file_name: str) -> bool:
    """Check if a metadata file exists in a folder."""
    try:
        files = os.listdir(metadata_folder_path)
        exists = metadata_file_name in files
        LOGGER.debug("Metadata file '%s' exists=%s in '%s'", metadata_file_name, exists, metadata_folder_path)
        return exists
    except Exception as exc:
        LOGGER.exception("Error checking metadata in folder: %s", exc)
        raise


def read_json(json_file_name: str) -> Dict[str, Any]:
    """Read a JSON file into a Python dictionary."""
    try:
        with open(json_file_name, "r") as f:
            data = json.load(f)
        LOGGER.debug("Loaded JSON file: %s", json_file_name)
        return data
    except FileNotFoundError:
        LOGGER.exception("JSON file not found: %s", json_file_name)
        raise
    except json.JSONDecodeError as exc:
        LOGGER.exception("Invalid JSON in file %s: %s", json_file_name, exc)
        raise
    except Exception as exc:
        LOGGER.exception("Failed to read JSON file %s: %s", json_file_name, exc)
        raise


def read_metadata_file(metadata_folder_path: str, metadata_file_name: str) -> Dict[str, Any]:
    """Read a metadata JSON file from a folder path and filename."""
    try:
        full_path = os.path.join(metadata_folder_path, metadata_file_name)
        LOGGER.info("Reading metadata file: %s", full_path)
        return read_json(full_path)
    except Exception:
        LOGGER.exception("Failed to read metadata file from %s", metadata_folder_path)
        raise


def read_bronze_metadata(path_to_bronze_metadata_folder: str, bronze_metadata_name: str) -> Dict[str, Any]:
    """Convenience wrapper to read *bronze* layer metadata."""
    return read_metadata_file(metadata_folder_path=path_to_bronze_metadata_folder,
                              metadata_file_name=bronze_metadata_name)


def df_natural_key(df: DataFrame,
                   primary_natural_name: str,
                   df_schema: StructType) -> DataFrame:
    """Create a deterministic natural key hash column from provided schema fields."""
    try:
        columns_to_pick = [f.name for f in df_schema.fields]
        df_select = df.select(*columns_to_pick).dropDuplicates()
        
        df_with_key = df_select.withColumn(
            primary_natural_name,
            F.sha2(F.concat_ws("||", *df_select.columns), 256)
        )
        LOGGER.info("Natural key '%s' created with %d source columns.", primary_natural_name, len(columns_to_pick))
        return df_with_key
    except Exception as exc:
        LOGGER.exception("Failed creating natural key '%s': %s", primary_natural_name, exc)
        raise


def download_data(path_to_kaggle: str) -> str:
    """Download a Kaggle dataset using kagglehub."""
    try:
        import kagglehub  # type: ignore
    except Exception as exc:
        LOGGER.exception("kagglehub import failed: %s", exc)
        raise

    try:
        LOGGER.info("Downloading Kaggle dataset: %s", path_to_kaggle)
        path = kagglehub.dataset_download(path_to_kaggle)
        LOGGER.info("Dataset downloaded to: %s", path)
        return path
    except Exception as exc:
        LOGGER.exception("Failed to download dataset %s: %s", path_to_kaggle, exc)
        raise


def transform_order_date(df: DataFrame,
                         order_column: str,
                         new_column_order_date: str) -> DataFrame:
    """Parse an order date column and derive year/month/day convenience columns."""
    try:
        out = df.withColumn(new_column_order_date, F.to_timestamp(order_column, "dd/MM/yyyy"))
        ymd = {
            "order_year": F.date_format(F.col(new_column_order_date), "yyyy"),
            "order_month": F.date_format(F.col(new_column_order_date), "MM"),
            "order_day": F.date_format(F.col(new_column_order_date), "dd"),
        }
        out = out.withColumns(ymd).drop(new_column_order_date)
        LOGGER.debug("Transformed order date from '%s' into '%s' (+year/month/day).", order_column, new_column_order_date)
        return out
    except Exception as exc:
        LOGGER.exception("Failed transforming order date: %s", exc)
        raise


def load_previous_df(path_to_old_df: str) -> Optional[DataFrame]:
    """Attempt to read a Parquet DataFrame if it exists; otherwise return None."""
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            LOGGER.warning("No active SparkSession found; cannot read previous DataFrame.")
            return None
        LOGGER.info("Reading previous DataFrame from: %s", path_to_old_df)
        df = spark.read.parquet(path_to_old_df)
        return df
    except Exception as exc:
        LOGGER.warning("Could not load previous DataFrame at '%s': %s", path_to_old_df, exc)
        return None


def find_missing_rows(df_old: DataFrame, df_new: DataFrame, key_col: str) -> DataFrame:
    """Returns rows that exist in df_old but are missing in df_new based on a unique key column."""
    diff = (
        df_old.alias("a")
        .join(df_new.alias("b"), on=key_col, how="left")
        .filter(F.col(f"b.{key_col}").isNull())
        .select("b.*")
    )
    return diff



def get_df_not_present(path_to_old_df: str, new_df: DataFrame, uniq_column: str) -> Tuple[DataFrame, bool]:
    """Compare an existing DataFrame with a new one and return records not present in the new set."""
    try:
        old_df = load_previous_df(path_to_old_df=path_to_old_df)
        if old_df is not None:
            diff_df = find_missing_rows(old_df, new_df, uniq_column)
            if diff_df.rdd.isEmpty():
                LOGGER.info("No missing records — the DataFrame is empty.")
                return new_df, False
            else:
                LOGGER.info("Missing and/or new records found compared to previous dataset.")
                return diff_df, True
        LOGGER.info("Previous DataFrame not found; treating all rows as new.")
        return new_df, True
    except Exception as exc:
        LOGGER.exception("Failed while computing DataFrame difference: %s", exc)
        return new_df, True


def write_json_out(dict_: Dict[str, Any], file_name: str) -> None:
    """Write a dictionary to disk as JSON."""
    try:
        with open(file_name, "w") as f:
            json.dump(dict_, f)
        LOGGER.info("JSON written to %s", file_name)
    except Exception as exc:
        LOGGER.exception("Failed to write JSON to %s: %s", file_name, exc)
        raise


def merge_metadata(meta1: Dict[str, Any], meta2: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two metadata dictionaries with a 'bronze_files_list' key."""
    try:
        merged = deepcopy(meta1)  # keep original safe
        merged_dict = {
            list(item.keys())[0]: list(item.values())[0]
            for item in merged.get("bronze_files_list", [])
        }

        for entry in meta2.get("bronze_files_list", []):
            for path, info in entry.items():
                merged_dict[path] = info  # overwrite or add

        merged["bronze_files_list"] = [{path: info} for path, info in merged_dict.items()]
        LOGGER.debug("Merged metadata with %d bronze file entries.", len(merged["bronze_files_list"]))
        return merged
    except Exception as exc:
        LOGGER.exception("Failed to merge metadata: %s", exc)
        raise


def get_schema_diff(original_schema: StructType, new_schema: StructType) -> bool:
    """Compare two Spark schemas and log differences."""
    try:
        fields1 = {f.name for f in original_schema.fields}
        fields2 = {f.name for f in new_schema.fields}

        missing_in_schema1 = fields2 - fields1

        if missing_in_schema1:
            LOGGER.warning("There are additional columns in the new data: %s", missing_in_schema1)
        else:
            LOGGER.info("No additional columns detected.")

        fields1_dict = {f.name: str(f.dataType) for f in original_schema.fields}
        fields2_dict = {f.name: str(f.dataType) for f in new_schema.fields}

        type_mismatch = {
            name: (fields1_dict[name], fields2_dict[name])
            for name in fields1_dict.keys() & fields2_dict.keys()
            if fields1_dict[name] != fields2_dict[name]
        }
        if type_mismatch:
            LOGGER.warning("Type mismatches detected: %s", type_mismatch)
        else:
            LOGGER.info("No type mismatches detected.")

        return bool(missing_in_schema1 or type_mismatch)
    except Exception as exc:
        LOGGER.exception("Schema diff check failed: %s", exc)
        return True


def convert_df_columns(df: DataFrame) -> Tuple[DataFrame, StructType]:
    """Normalize column names to lowercase with underscores and drop 'row_id' from schema."""
    try:
        columns = df.columns
        columns_to_rename = {col_name: col_name.lower().replace(" ", "_").replace("-", "_") for col_name in columns}
        df_new = df.withColumnsRenamed(columns_to_rename)

        if "row_id" in df_new.columns:
            df_schema = df_new.drop("row_id").schema
        else:
            df_schema = df_new.schema

        LOGGER.debug("Converted %d columns to normalized snake_case.", len(columns_to_rename))
        return df_new, df_schema
    except Exception as exc:
        LOGGER.exception("Failed to convert DataFrame columns: %s", exc)
        raise
