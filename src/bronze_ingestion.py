import sys
import platform
import os
import json
from typing import Dict, List, Tuple, Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils.bronze_utils import (
    check_metadata_in_folder, read_metadata_file, read_bronze_metadata,
    get_file_info, convert_df_columns, transform_order_date, df_natural_key,
    load_previous_df, find_missing_rows, get_df_not_present, write_json_out,
    get_schema_diff, merge_metadata, download_data
)


from utils.general_utils import get_logger, build_spark, add_ingestion_metadata

LOG_LEVEL = "INFO"
log = get_logger(level=LOG_LEVEL)


def get_files_path(raw_download_base_path: str,
                   path_to_bronze_metadata: str,
                   bronze_metadata_name: str) -> Tuple[List[str], bool, Dict[str, Any], Optional[Dict[str, Any]]]:
    """Collect candidate input files and prepare bronze metadata stubs."""
    from collections import defaultdict

    files_kaggle_download_list: List[str] = []
    metadata_dict: Dict[str, Any] = defaultdict(list)
    bronze_metadata_present = False
    old_metadata_dict: Optional[Dict[str, Any]] = None

    try:
        if check_metadata_in_folder(path_to_bronze_metadata, bronze_metadata_name):
            old_metadata_dict = read_bronze_metadata(
                path_to_bronze_metadata_folder=path_to_bronze_metadata,
                bronze_metadata_name=bronze_metadata_name
            )
            bronze_metadata_present = True
            metadata_file = old_metadata_dict
            log.info("Existing bronze metadata found at %s", path_to_bronze_metadata)
        else:
            metadata_file = None
            log.info("No existing bronze metadata found at %s", path_to_bronze_metadata)

        for file in os.listdir(raw_download_base_path):
            each_files_path = os.path.join(raw_download_base_path, file)

            if not bronze_metadata_present:
                file_info = get_file_info(each_files_path)
                metadata_dict["bronze_files_list"].append(file_info)
                files_kaggle_download_list.append(each_files_path)
            else:
                bronze_ingested_files = metadata_file.get("bronze_files_list", [])
                each_file_metadata = next(
                    (info for entry in bronze_ingested_files for path, info in entry.items() if path == each_files_path),
                    {}
                )

                new_file_metadata = get_file_info(each_files_path)
                if each_file_metadata:
                    changed = (
                        each_file_metadata.get("modified") != new_file_metadata[each_files_path]["modified"]
                        or each_file_metadata.get("size_bytes") != new_file_metadata[each_files_path]["size_bytes"]
                    )
                    not_written = not each_file_metadata.get("data_written", False)

                    if changed and not_written:
                        files_kaggle_download_list.append(each_files_path)
                        metadata_dict["bronze_files_list"].append(new_file_metadata)
                    else:
                        log.info("File already processed or unchanged: %s", each_files_path)
                else:
                    files_kaggle_download_list.append(each_files_path)
                    metadata_dict["bronze_files_list"].append(new_file_metadata)

        new_files = bool(files_kaggle_download_list)
        return files_kaggle_download_list, new_files, metadata_dict, old_metadata_dict
    except Exception as exc:
        log.exception("Failed to build file list and metadata stubs: %s", exc)
        return [], False, {}, old_metadata_dict


def run_download_bronze(raw_download_base_path: str,
                        path_to_bronze_metadata: str,
                        bronze_metadata_name: str = "bronze_metadata.json",
                        spark: Optional[SparkSession] = None
                        ) -> Tuple[Optional[DataFrame], Optional[Dict[str, Any]], Optional[T.StructType], Optional[bool], Optional[Dict[str, Any]]]:
    """Create a unified DataFrame from raw files and enrich with standard bronze columns."""
    try:
        files_list, new_file_exist, metadata_file, old_metadata_file = get_files_path(
            raw_download_base_path=raw_download_base_path,
            path_to_bronze_metadata=path_to_bronze_metadata,
            bronze_metadata_name=bronze_metadata_name
        )
        log.info("New files detected: %s", new_file_exist)

        if not files_list:
            log.info("No files to process from %s", raw_download_base_path)
            return None, None, None, new_file_exist, old_metadata_file

        if spark is None:
            spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active SparkSession available for reading files.")

        df = spark.read.csv(files_list, inferSchema=True, header=True)
        log.debug("Read CSVs with columns: %s", df.columns)

        df_2, df_schema = convert_df_columns(df)

        if old_metadata_file and "bronze_schema" in old_metadata_file:
            old_schema = T.StructType.fromJson(json.loads(old_metadata_file["bronze_schema"]))
            df_ft = df_natural_key(df_2, "nk", df_schema)
            df_final = transform_order_date(df_ft, "order_date", "order_date_timestamp")
            return df_final, metadata_file, df_schema, new_file_exist, old_metadata_file
        else:
            df_ft = df_natural_key(df_2, "nk", df_schema)
            df_final = transform_order_date(df_ft, "order_date", "order_date_timestamp")
            return df_final, metadata_file, df_schema, new_file_exist, old_metadata_file
    except Exception as exc:
        log.exception("run_download_bronze failed: %s", exc)
        return None, None, None, None, None


def write_df_meta_out(df: DataFrame,
                      mode: str,
                      df_schema: T.StructType,
                      metadata_file: Dict[str, Any],
                      metadata_file_name: str,
                      path_folder_out: str,
                      merge_schema: bool = False,
                      merge_metadata_: bool = False,
                      old_metadata_file: Optional[Dict[str, Any]] = None) -> None:
    """Write the bronze DataFrame and update the bronze metadata JSON."""
    try:
        # ✅ add ingestion metadata before writing
        df = add_ingestion_metadata(df, path_folder_out)

        writer = df.write.partitionBy("order_year", "order_month", "order_day").mode(mode)
        if merge_schema:
            writer = writer.option("mergeSchema", "true")

        out_path = os.path.join(path_folder_out, "raw")
        writer.parquet(out_path)
        log.info("Wrote bronze data to %s (mode=%s, merge_schema=%s)", out_path, mode, merge_schema)

        br_fl = metadata_file.get("bronze_files_list", [])
        for item in br_fl:
            for path_info in item.values():
                path_info["data_written"] = True

        if merge_metadata_ and old_metadata_file:
            merged_metadata = merge_metadata(old_metadata_file, metadata_file)
            metadata_out = merged_metadata
        else:
            metadata_file["bronze_schema"] = df_schema.json()
            metadata_out = metadata_file

        metadata_file_path = os.path.join(path_folder_out, metadata_file_name)
        write_json_out(metadata_out, metadata_file_path)
        log.info("Wrote bronze metadata to %s", metadata_file_path)
    except Exception as exc:
        log.exception("write_df_meta_out failed: %s", exc)
        raise


def read_and_write_raw_data(raw_download_base_path: str,
                            path_to_bronze_metadata: str,
                            metadata_file_name: str,
                            spark: SparkSession) -> Tuple[str, bool]:
    """End-to-end bronze ingestion: detect, read, transform, write, and update metadata."""
    try:
        log.info("Starting bronze ingestion from %s", raw_download_base_path)
        df, metadata_file, df_schema, new_file_exist, old_metadata_file = run_download_bronze(
            raw_download_base_path=raw_download_base_path,
            path_to_bronze_metadata=path_to_bronze_metadata,
            bronze_metadata_name=metadata_file_name,
            spark=spark
        )
        log.info("Completed file discovery and initial transforms. new_file_exist=%s", new_file_exist)

        if df is not None and old_metadata_file:
            old_schema = T.StructType.fromJson(json.loads(old_metadata_file["bronze_schema"]))
            diff_schema = get_schema_diff(old_schema, df_schema)
            df_out, new_records = get_df_not_present(os.path.join(path_to_bronze_metadata, "raw"), df, uniq_column="nk")

            if new_records and not df_out.rdd.isEmpty():
                mode = "append"
                if diff_schema:
                    log.info("Writing with schema evolution enabled.")
                    write_df_meta_out(df_out, mode=mode, df_schema=df_schema,
                                      metadata_file_name=metadata_file_name, merge_schema=True, merge_metadata_=True,
                                      metadata_file=metadata_file, old_metadata_file=old_metadata_file,
                                      path_folder_out=path_to_bronze_metadata)
                else:
                    log.info("Writing without schema evolution.")
                    write_df_meta_out(df_out, mode=mode, df_schema=df_schema,
                                      merge_schema=False, merge_metadata_=True,
                                      metadata_file_name=metadata_file_name,
                                      metadata_file=metadata_file, old_metadata_file=old_metadata_file,
                                      path_folder_out=path_to_bronze_metadata)

        elif df is not None and not old_metadata_file:
            log.info("Initial bronze write (no prior metadata present).")
            mode = "overwrite"
            write_df_meta_out(df, mode=mode, df_schema=df_schema, merge_schema=False, merge_metadata_=False,
                              metadata_file_name=metadata_file_name, metadata_file=metadata_file,
                              old_metadata_file={}, path_folder_out=path_to_bronze_metadata)

        else:
            log.info("No new data to write.")
            return os.path.join(path_to_bronze_metadata, "raw"), False

        return os.path.join(path_to_bronze_metadata, "raw"), True
    except Exception as exc:
        log.exception("read_and_write_raw_data failed: %s", exc)
        return os.path.join(path_to_bronze_metadata, "raw"), False


def main() -> None:
    """CLI entry point for bronze ingestion (Spark + Kaggle)."""
    try:
        log.info("Python=%s | Platform=%s", sys.version.split()[0], platform.platform())

        spark = build_spark(local=True)
        path_to_kaggle = "rohitsahoo/sales-forecasting"
        log.info("Starting Kaggle download for %s", path_to_kaggle)

        raw_download_base_path = download_data(path_to_kaggle=path_to_kaggle)
        log.info("Kaggle dataset downloaded to %s", raw_download_base_path)

        path_to_bronze_metadata = os.path.join("/opt/workfold", "data/bronze")
        os.makedirs(path_to_bronze_metadata, exist_ok=True)

        path_to_bronze_folder, written_to_bronze = read_and_write_raw_data(
            raw_download_base_path, path_to_bronze_metadata,
            metadata_file_name="bronze_metadata.json", spark=spark
        )
        log.info("Bronze path=%s | written=%s", path_to_bronze_folder, written_to_bronze)
        return path_to_bronze_folder, written_to_bronze
    except Exception as exc:
        log.exception("Fatal error in main: %s", exc)


if __name__ == "__main__":
    path_to_bronze_folder, written_to_bronze = main()
