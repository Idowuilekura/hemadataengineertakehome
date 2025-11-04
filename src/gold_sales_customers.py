from  utils.gold_utils_new import read_gold_df, split_data_sales_cust,read_old_and_new_df, log, spark
from utils.bronze_utils import df_natural_key, get_df_not_present, write_json_out, read_json
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F 

def compute_customer_order_metrics(
    sales_df,
    cust_df,
):
    """
    Join sales and customer data to compute per-customer order metrics over
    rolling time windows relative to the latest `order_date` present in the input.

    Args:
        sales_df (DataFrame): Sales/order fact-like DataFrame containing at least:
            - order_id, order_date, customer_id, city
            - order_year, order_month, order_day (for downstream partitioning)
        cust_df (DataFrame): Customer dimension-like DataFrame containing at least:
            - customer_id, customer_first_name, customer_last_name, customer_segment, country

    Returns:
        DataFrame: Aggregated DataFrame grouped by customer and date parts with:
            - qty_orders_last_1m, qty_orders_last_6m, qty_orders_last_12m, qty_orders_all_time

    Logging:
        - Logs start/end of computation.
        - Logs derived latest reference date and window bounds.

    Exceptions:
        - Any exception is logged and re-raised to surface upstream.
    """
    try:
        log.info("Starting compute_customer_order_metrics: joining sales and customers.")
        # Join on customer_id (non-destructive)
        joined_df = sales_df.join(cust_df, on="customer_id", how="inner")

        log.debug("Computing latest order_date from joined dataset.")
        latest_date_value = joined_df.agg(F.max("order_date").alias("max_date")).collect()[0]["max_date"]
        latest_date = F.lit(latest_date_value).cast("date")
        log.info(f"Latest order_date detected: {latest_date_value}")

        # Define rolling window lower bounds relative to latest_date
        lower_1m  = F.add_months(latest_date, -1)
        lower_6m  = F.add_months(latest_date, -6)
        lower_12m = F.add_months(latest_date, -12)
        log.debug("Computed rolling window bounds for 1m/6m/12m.")

        # Compute order metrics per customer
        customer_orders_df = (
            joined_df.groupBy(
                "customer_id",
                "customer_first_name",
                "customer_last_name",
                "customer_segment",
                "country",
                "city",
                "order_year",
                "order_month",
                "order_day"
            )
            .agg(
                F.sum(
                    F.when(F.col("order_date").between(lower_1m, latest_date), 1).otherwise(0)
                ).alias("qty_orders_last_1m"),

                F.sum(
                    F.when(F.col("order_date").between(lower_6m, latest_date), 1).otherwise(0)
                ).alias("qty_orders_last_6m"),

                F.sum(
                    F.when(F.col("order_date").between(lower_12m, latest_date), 1).otherwise(0)
                ).alias("qty_orders_last_12m"),

                F.count("order_id").alias("qty_orders_all_time")
            )
        )

        log.info("Completed compute_customer_order_metrics.")
        return customer_orders_df
    except Exception as e:
        log.exception("Failed in compute_customer_order_metrics.")
        raise

def read_cust_sales_compute(silver_data_path,sales_cols,cust_cols,spark):
    """
    Read gold (derived from silver) dataset, split into sales and customer DataFrames,
    and compute the aggregated customer order metrics.

    Args:
        silver_data_path (str): Base path to silver-derived input used by `read_gold_df`.
        sales_cols (list[str]): Column subset for sales DataFrame.
        cust_cols (list[str]): Column subset for customer DataFrame.
        spark (SparkSession): Active Spark session.

    Returns:
        Tuple[DataFrame, DataFrame]: (sales_df, cust_df_agg)

    Logging:
        - Logs I/O paths and schema column selections.

    Exceptions:
        - Any exception is logged and re-raised.
    """
    try:
        log.info(f"Reading gold dataset from: {silver_data_path}")
        gold_df = read_gold_df(silver_data_path,spark=spark)

        log.debug(f"Splitting dataset into sales ({len(sales_cols)} cols) and customer ({len(cust_cols)} cols).")
        sales_df, cust_df = split_data_sales_cust(gold_df, cust_cols, sales_cols)

        log.info("Computing aggregated customer order metrics.")
        cust_df_agg = compute_customer_order_metrics(sales_df,cust_df)
        log.info("Finished computing aggregated customer order metrics.")
        return sales_df, cust_df_agg
    except Exception:
        log.exception("Failed in read_cust_sales_compute.")
        raise

def get_df_natural(df):
    """
    Add a natural key column 'nk' to the provided DataFrame based on its schema.

    Args:
        df (DataFrame): Input DataFrame to key.

    Returns:
        DataFrame: DataFrame with added 'nk' natural key column.

    Logging:
        - Logs schema fields used for natural key generation.

    Exceptions:
        - Any exception is logged and re-raised.
    """
    try:
        schema = df.schema 
        log.debug(f"Generating natural key 'nk' with schema fields: {[f.name for f in schema]}")
        df_with_nat_key = df_natural_key(df, "nk", schema)
        log.info("Natural key 'nk' added to DataFrame.")
        return df_with_nat_key
    except Exception:
        log.exception("Failed in get_df_natural.")
        raise

def write_sales_out(df_name, path_to_output,folder_name,metadata_file_name,df=None):
    """
    Write sales dataset with partitioning (year/month/day), handling append vs overwrite
    using metadata-based change detection.

    Args:
        df_name (str): Logical dataset name ("sales").
        path_to_output (str): Base gold output path.
        folder_name (str): Folder name for this dataset (e.g., 'sales_dataset').
        metadata_file_name (str): Metadata JSON file name (e.g., 'sales_metadata.json').
        df (DataFrame, optional): DataFrame to write. If None, relies on metadata function.

    Side Effects:
        - Writes Parquet files partitioned by order_year, order_month, order_day.
        - Updates metadata JSON (data_path, data_written flags).

    Logging:
        - Logs metadata paths, write mode (append/overwrite), and success/failure.

    Exceptions:
        - Exceptions are caught and logged; function continues where safe.
    """
    metadata_file_path = os.path.join(path_to_output, folder_name)
    path_to_old_df = os.path.join(path_to_output, folder_name)
    metadata_folder_path = path_to_output 

    try:
        log.info(f"Evaluating write plan for '{df_name}' at {metadata_file_path} with metadata file '{metadata_file_name}'.")
        df, new_records, append = read_old_and_new_df(metadata_file_path, metadata_file_name, path_to_old_df, df)
        metadata_full_file_path = os.path.join(metadata_file_path, metadata_file_name)
        try:
            metadata_dict = read_json(metadata_full_file_path)
            log.debug(f"Loaded existing metadata from: {metadata_full_file_path}")
        except Exception:
            metadata_dict = {}
            log.info("No existing metadata found; will initialize a fresh metadata dict.")

        if df is not None and new_records:
            try:
                _ = df.columns  # sanity access to trigger if df is valid
                mode = "append" if append else "overwrite"
                target_path = os.path.join(path_to_output,folder_name)
                log.info(f"Writing sales data in '{mode}' mode to: {target_path}")

                df.write.partitionBy("order_year","order_month","order_day").mode(mode).parquet(target_path)

                if append:
                    metadata_dict["data_written"] = True 
                else:
                    metadata_dict = {}
                    metadata_dict["data_path"] = target_path
                    metadata_dict["data_written"] = True 

                write_json_out(metadata_dict, metadata_full_file_path)
                log.info(f"Sales data write successful. Metadata updated at: {metadata_full_file_path}")
            except Exception:
                log.exception("Write attempted but failed for sales dataset.")
        else:
            log.info("No new sales data to write.")
    except Exception:
        log.exception("Failed in write_sales_out (preparation or metadata handling).")

def write_data_out(df_name, path_to_output,df):
    """
    Dispatch writer for customer and sales datasets.

    Args:
        df_name (str): 'customer' or 'sales'.
        path_to_output (str): Base gold output path.
        df (DataFrame): DataFrame to write.

    Behavior:
        - 'customer': overwrite partitioned Parquet to {path_to_output}/customer_dataset
        - 'sales': delegate to write_sales_out with metadata-aware append/overwrite

    Logging:
        - Logs the selected branch and target paths.

    Exceptions:
        - Exceptions are caught and logged; function continues where safe.
    """
    try:
        if df_name == "customer":
            folder_name = df_name + "_dataset"
            target_path = os.path.join(path_to_output,folder_name)
            log.info(f"Writing customer dataset (overwrite) to: {target_path}")
            df.write.partitionBy("order_year","order_month","order_day").mode("overwrite").parquet(target_path)
            log.info("Customer dataset write completed.")
        elif df_name == "sales":
            folder_name = df_name + "_dataset"
            log.info("Delegating sales dataset write to write_sales_out with metadata management.")
            write_sales_out(df_name="sales",path_to_output=path_to_output,folder_name=folder_name,metadata_file_name="sales_metadata.json",df=df)
        else:
            log.error(f"Unknown df_name '{df_name}'. Expected 'customer' or 'sales'.")
    except Exception:
        log.exception("Failed in write_data_out.")


# try:
#     sales_cols = ["order_id","order_date","customer_id","shipment_date", "shipment_mode","city","order_year","order_month","order_day"]
#     cust_cols = ["customer_id","customer_first_name","customer_last_name","customer_segment","country"]

#     silver_data_path = "/opt/workfold/silver/silver"
#     gold_folder_path = "/opt/workfold/gold_folder"

#     log.info("Starting end-to-end pipeline: read -> compute -> natural key -> write.")

#     sales_df, cust_df_agg = read_cust_sales_compute(
#         silver_data_path=silver_data_path,
#         sales_cols=sales_cols,
#         cust_cols=cust_cols,
#         spark=spark
#     )

#     sales_df_natural = get_df_natural(sales_df)
#     customer_df_natural = get_df_natural(cust_df_agg)

#     log.info("Writing customer (built from sales_df_natural) and sales (built from customer_df_natural) datasets.")
#     write_data_out(df_name="customer",path_to_output=gold_folder_path, df=sales_df_natural)
#     write_data_out(df_name="sales",path_to_output=gold_folder_path, df=customer_df_natural)

#     log.info("Pipeline completed successfully.")
# except Exception:
#     log.exception("Pipeline execution failed at top level.")


def run_gold_pipeline():
    """
    Execute the end-to-end gold-layer pipeline:
      read -> compute -> natural key -> write.

    This function wraps the existing top-level try/except execution block so it
    can be invoked programmatically (e.g., from a scheduler or notebook cell)
    without refactoring internal logic.

    Behavior:
      - Reads gold (derived from silver) input using configured paths.
      - Splits into sales and customer DataFrames.
      - Computes customer order metrics.
      - Adds natural keys.
      - Writes partitioned Parquet outputs for customer and sales datasets.
      - Logs progress at each major step and captures any exceptions.

    Logging:
      - Uses the existing `log` logger.
      - On failure, logs a stack trace with contextual message.

    Exceptions:
      - All exceptions are caught and logged internally to preserve the original
        top-level behavior. No exception is re-raised from this function.
    """
    try:
        sales_cols = ["order_id","order_date","customer_id","shipment_date", "shipment_mode","city","order_year","order_month","order_day"]
        cust_cols = ["customer_id","customer_first_name","customer_last_name","customer_segment","country"]

        silver_data_path = "/opt/workfold/silver/silver"
        gold_folder_path = "/opt/workfold/gold_folder"

        log.info("Starting end-to-end pipeline: read -> compute -> natural key -> write.")

        sales_df, cust_df_agg = read_cust_sales_compute(
            silver_data_path=silver_data_path,
            sales_cols=sales_cols,
            cust_cols=cust_cols,
            spark=spark
        )

        sales_df_natural = get_df_natural(sales_df)
        customer_df_natural = get_df_natural(cust_df_agg)

        log.info("Writing customer (built from sales_df_natural) and sales (built from customer_df_natural) datasets.")
        write_data_out(df_name="customer",path_to_output=gold_folder_path, df=sales_df_natural)
        write_data_out(df_name="sales",path_to_output=gold_folder_path, df=customer_df_natural)

        log.info("Pipeline completed successfully.")
    except Exception:
        log.exception("Pipeline execution failed at top level.")


if __name__ == "__main__":
    run_gold_pipeline()