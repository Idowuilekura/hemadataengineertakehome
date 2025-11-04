from bronze import main
import os
import datetime
from utils.bronze_utils import (check_metadata_in_folder, read_json, df_natural_key, write_json_out,
                                get_df_not_present, read_metadata_file, get_schema_diff)
from utils.silver_utils import (log, build_spark, non_empty, cast_and_collect_bad, add_sales_features, get_df_schema,
                                schema_json_to_struct, silver_column_clean_enrich_schema)

from pyspark.sql import types as T
import traceback


# Initialize Spark and Bronze pathing up front (original behavior preserved)
spark = build_spark(local=True)
path_to_bronze_data_folder, written_to_bronze = main()
path_to_bronze_metadata = path_to_bronze_data_folder[:-4]
if written_to_bronze:
    df = spark.read.parquet(path_to_bronze_data_folder)


def read_bronze_data(path_to_bronze_metadata, bronze_metadata_file_name, written_to_bronze, spark):
    """
    Read Bronze-layer data using the schema recorded in the Bronze metadata file.

    Parameters
    ----------
    path_to_bronze_metadata : str
        Path to the Bronze layer folder containing the metadata file.
    bronze_metadata_file_name : str
        Filename of the Bronze metadata JSON (e.g., "bronze_metadata.json").
    written_to_bronze : bool
        Flag indicating whether Bronze data was written in the current execution.
    spark : pyspark.sql.SparkSession
        Active Spark session used to read the Bronze dataset.

    Returns
    -------
    pyspark.sql.DataFrame
        Bronze DataFrame read with an augmented schema (prepended 'row_id').
        Returns None if the required metadata is missing or if nothing was written.
    """
    try:
        if written_to_bronze and check_metadata_in_folder(metadata_folder_path=path_to_bronze_metadata,
                                                          metadata_file_name=bronze_metadata_file_name):
            metadata_full_file_path = os.path.join(path_to_bronze_metadata, bronze_metadata_file_name)
            metadata_file = read_json(metadata_full_file_path)
            bronze_schema = schema_json_to_struct(metadata_file["bronze_schema"])
            new_schema = T.StructType([T.StructField("row_id", T.IntegerType(), True)] + bronze_schema.fields)
            log.info("------------------------------------------")
            log.info("Bronze data path: %s", path_to_bronze_data_folder)
            silver_df = spark.read.parquet(path_to_bronze_data_folder, schema=new_schema)
            log.info("Skipping re-download; Bronze data already written for this run.")
            return silver_df
        else:
            log.warning("Bronze data not written or metadata file missing: %s", bronze_metadata_file_name)
            return None
    except Exception as exc:
        log.exception("read_bronze_data failed: %s", exc)
        return None


def read_bronze_silver(path_to_bronze_metadata, bronze_metadata_file_name, written_to_bronze, date_cols, float_cols, spark):
    """
    Read from Bronze and apply Silver-layer cleaning/enrichment.

    Parameters
    ----------
    path_to_bronze_metadata : str
        Path to the Bronze layer folder.
    bronze_metadata_file_name : str
        Bronze metadata JSON filename.
    written_to_bronze : bool
        Whether Bronze data was written in this run.
    date_cols : list[str]
        Date columns to validate and cast at the Silver stage.
    float_cols : list[str]
        Numeric columns to validate and cast at the Silver stage.
    spark : pyspark.sql.SparkSession
        Active Spark session.

    Returns
    -------
    tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame, pyspark.sql.types.StructType]
        (df_with_nk, bad_df, silver_schema)
    """
    try:
        df = read_bronze_data(path_to_bronze_metadata, bronze_metadata_file_name, written_to_bronze, spark)
        df, bad_df, silver_schema = silver_column_clean_enrich_schema(df, date_cols, float_cols,
                                                                      date_fmt="dd/MM/yyyy", dedup_bad=True)
        df_add_natural_key = df_natural_key(df, "nk", silver_schema)
        return df_add_natural_key, bad_df, silver_schema
    except Exception as exc:
        log.exception("read_bronze_silver failed: %s", exc)
        raise


def silver_stand_write_out(df, old_metadata_dict, silver_output_data_path, silver_folder_name, new_schema,
                           silver_metadata_name="silver_metadata.json", schema_drift=False, silver_data_exist=False):
    """
    Write Silver-layer DataFrame to partitioned Parquet and update Silver metadata JSON.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame to write.
    old_metadata_dict : dict
        Existing Silver metadata dictionary to update.
    silver_output_data_path : str
        Parent folder where Silver outputs are written.
    silver_folder_name : str
        Name of the Silver subfolder (e.g., "silver").
    new_schema : pyspark.sql.types.StructType
        Schema for this Silver write.
    silver_metadata_name : str, optional
        Metadata JSON filename, by default "silver_metadata.json".
    schema_drift : bool, optional
        Whether schema evolution should be enabled on write, by default False.
    silver_data_exist : bool, optional
        Whether Silver data already exists (append) or not (overwrite), by default False.

    Returns
    -------
    None
    """
    try:
        silver_full_data_output = os.path.join(silver_output_data_path, silver_folder_name)
        if silver_data_exist:
            if schema_drift:
                df.write.partitionBy("order_year", "order_month", "order_day").mode("append").option("mergeSchema", "true").parquet(silver_full_data_output)
                silver_schema_json = new_schema.json()
                silver_metadata_json = old_metadata_dict
                silver_metadata_json["silver_schema"] = silver_schema_json
                silver_metadata_json["total_columns_written"] = len(df.columns)
                total_rows_old_written = silver_metadata_json["total_rows_written"]
                silver_metadata_json["total_rows_written"] = total_rows_old_written + df.count()
                silver_metadata_json["execution_time"] = datetime.datetime.now().isoformat()
                full_silver_metadate_path = os.path.join(silver_output_data_path, silver_metadata_name)
                write_json_out(silver_metadata_json, full_silver_metadate_path)
                log.info("Silver write (append + schema evolution) completed: %s", silver_full_data_output)
            else:
                df.write.partitionBy("order_year", "order_month", "order_day").mode("append").parquet(silver_full_data_output)
                silver_metadata_json = old_metadata_dict
                silver_metadata_json["total_columns_written"] = len(df.columns)
                total_rows_old_written = silver_metadata_json["total_rows_written"]
                silver_metadata_json["total_rows_written"] = total_rows_old_written + df.count()
                silver_metadata_json["execution_time"] = datetime.datetime.now().isoformat()
                full_silver_metadate_path = os.path.join(silver_output_data_path, silver_metadata_name)
                write_json_out(silver_metadata_json, full_silver_metadate_path)
                log.info("Silver write (append) completed: %s", silver_full_data_output)
        else:
            df.write.partitionBy("order_year", "order_month", "order_day").mode("overwrite").parquet(silver_full_data_output)
            silver_schema_json = new_schema.json()
            silver_metadata_json = {}
            silver_metadata_json["silver_schema"] = silver_schema_json
            silver_metadata_json["data_written"] = True
            silver_metadata_json["total_columns_written"] = len(df.columns)
            silver_metadata_json["total_rows_written"] = df.count()
            silver_metadata_json["execution_time"] = datetime.datetime.now().isoformat()
            silver_metadata_json["data_path"] = str(silver_full_data_output)
            full_silver_metadate_path = os.path.join(silver_output_data_path, silver_metadata_name)
            log.info("Silver metadata path: %s", full_silver_metadate_path)
            write_json_out(silver_metadata_json, full_silver_metadate_path)
            log.info("Silver write (overwrite) completed: %s", silver_full_data_output)
    except Exception as exc:
        log.exception("silver_stand_write_out failed: %s", exc)
        raise


def silver_df_write_out(silver_data_folder, silver_metadata_name, bronze_metadata_folder=path_to_bronze_metadata, spark=spark):
    """
    Orchestrate the Silver-layer write:
    - Read Bronze and transform to Silver
    - Append/overwrite Silver data
    - Update Silver metadata
    - Persist bad records (right after every Silver write)

    Parameters
    ----------
    silver_data_folder : str
        Output folder for Silver data (and bad data).
    silver_metadata_name : str
        Silver metadata JSON filename.
    bronze_metadata_folder : str, optional
        Bronze metadata folder path, defaults to global `path_to_bronze_metadata`.
    spark : pyspark.sql.SparkSession, optional
        Active Spark session, defaults to global `spark`.

    Returns
    -------
    None
    """
    try:
        log.info("Bronze metadata folder: %s", bronze_metadata_folder)
        df_add_natural_key, bad_df, silver_schema = read_bronze_silver(
            path_to_bronze_metadata=bronze_metadata_folder,
            bronze_metadata_file_name="bronze_metadata.json",
            written_to_bronze=True,
            date_cols=["order_date", "ship_date"],
            float_cols=["sales"],
            spark=spark
        )
        df = df_add_natural_key
        if check_metadata_in_folder(silver_data_folder, silver_metadata_name):
            silver_metadata_file = read_metadata_file(silver_data_folder, silver_metadata_name)
            old_silver_schema = schema_json_to_struct(silver_metadata_file["silver_schema"])
            old_silver_path = silver_metadata_file["data_path"]
            df, new_records_present = get_df_not_present(path_to_old_df=old_silver_path, new_df=df, uniq_column="nk")
            log.info("Candidate new/changed records: %s", df.count())
            schema_driff_presence = get_schema_diff(old_silver_schema, silver_schema)
            if schema_driff_presence and new_records_present and not df.rdd.isEmpty():
                log.info("Schema drift detected: writing with evolution.")
                silver_stand_write_out(df, old_metadata_dict=silver_metadata_file, silver_output_data_path=silver_data_folder,
                                       silver_folder_name="silver", new_schema=silver_schema, schema_drift=True, silver_data_exist=True)
                # Write bad data immediately after successful Silver write
                log.info("Writing out the bad data after Silver write.")
                silver_full_data_bad_output = os.path.join(silver_data_folder, "bad_data")
                bad_df.write.partitionBy("order_year", "order_month", "order_day").mode("overwrite").parquet(silver_full_data_bad_output)
            elif not schema_driff_presence and new_records_present and not df.rdd.isEmpty():
                log.info("No schema drift: appending to existing Silver dataset.")
                silver_stand_write_out(df, old_metadata_dict=silver_metadata_file, silver_output_data_path=silver_data_folder,
                                       silver_folder_name="silver", new_schema=silver_schema, schema_drift=False, silver_data_exist=True)
                # Write bad data immediately after successful Silver write
                log.info("Writing out the bad data after Silver write.")
                silver_full_data_bad_output = os.path.join(silver_data_folder, "bad_data")
                bad_df.write.partitionBy("order_year", "order_month", "order_day").mode("overwrite").parquet(silver_full_data_bad_output)
            else:
                log.info("No new records to write to Silver.")
        else:
            try:
                log.info("No Silver metadata file found; writing initial Silver dataset.")
                silver_stand_write_out(df, old_metadata_dict={}, silver_output_data_path=silver_data_folder,
                                       silver_folder_name="silver", new_schema=silver_schema, schema_drift=False, silver_data_exist=False)
                # Write bad data immediately after successful initial Silver write
                log.info("Writing out the bad data after Silver write.")
                silver_full_data_bad_output = os.path.join(silver_data_folder, "bad_data")
                bad_df.write.partitionBy("order_year", "order_month", "order_day").mode("overwrite").parquet(silver_full_data_bad_output)
            except Exception:
                log.exception("Error writing initial Silver dataset.")
    except Exception as exc:
        log.exception("silver_df_write_out failed: %s", exc)
        raise


# Parameters and execution
if __name__ == "__main__":
    silver_data_folder = os.path.join("/opt/workfold", "silver")
    silver_df_write_out(silver_data_folder, silver_metadata_name="silver_metadata.json")