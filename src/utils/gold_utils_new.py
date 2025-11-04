from __future__ import annotations

from pyspark.sql import SparkSession
import pyspark
from utils.bronze_utils import check_metadata_in_folder, get_df_not_present
from utils.general_utils import build_spark, get_logger
from typing import List, Tuple

log = get_logger()
spark = build_spark(local=True)


def read_gold_df(silver_data_path: str, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    """
    Read the Gold DataFrame from a Silver (Parquet) data path.

    Parameters
    ----------
    silver_data_path : str
        Path to the Parquet dataset (Silver output) to be read.
    spark : pyspark.sql.SparkSession
        Active Spark session to use for reading.

    Returns
    -------
    pyspark.sql.DataFrame
        The loaded Gold DataFrame.
    """
    try:
        gold_df = spark.read.parquet(silver_data_path)
        log.info("Loaded Gold DataFrame from: %s", silver_data_path)
        return gold_df
    except Exception as exc:
        log.exception("read_gold_df failed: %s", exc)
        raise


def split_data_sales_cust(gold_df: pyspark.sql.DataFrame,
                          cust_cols: List[str],
                          sales_cols: List[str]) -> Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
    """
    Split a single DataFrame into two DataFrames: sales_df and cust_df.

    Parameters
    ----------
    gold_df : pyspark.sql.DataFrame
        Input DataFrame to split.
    cust_cols : list[str]
        Column names to select for the customer DataFrame.
    sales_cols : list[str]
        Column names to select for the sales DataFrame.

    Returns
    -------
    tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]
        (sales_df, cust_df)
    """
    try:
        sales_df = gold_df.select(*sales_cols)
        cust_df = gold_df.select(*cust_cols)
        log.debug("Split DataFrame into sales (%d cols) and customer (%d cols).",
                  len(sales_cols), len(cust_cols))
        return sales_df, cust_df
    except Exception as exc:
        log.exception("split_data_sales_cust failed: %s", exc)
        raise


def read_old_and_new_df(metadata_folder_path: str,
                        metadata_file_name: str,
                        path_to_old_df: str,
                        df: pyspark.sql.DataFrame) -> Tuple[pyspark.sql.DataFrame, bool, bool]:
    """
    Compare an existing (old) Parquet dataset with a new DataFrame and decide append vs. initial write.

    Parameters
    ----------
    metadata_folder_path : str
        Folder path where the metadata JSON is stored.
    metadata_file_name : str
        Metadata filename to check for existence.
    path_to_old_df : str
        Path to the existing Parquet dataset to compare against.
    df : pyspark.sql.DataFrame
        New DataFrame to compare.

    Returns
    -------
    tuple[pyspark.sql.DataFrame, bool, bool]
        (df_to_write, new_records, append_flag)
        - df_to_write: filtered DataFrame containing only new/missing rows if metadata exists,
                       otherwise the original df.
        - new_records: True if there are records to write, else False.
        - append_flag: True if metadata exists (append), else False.
    """
    try:
        new_df = df
        append = False
        if check_metadata_in_folder(metadata_folder_path=metadata_folder_path,
                                    metadata_file_name=metadata_file_name):
            append = True
            df, new_records = get_df_not_present(path_to_old_df=path_to_old_df,
                                                         new_df=new_df,
                                                         uniq_column="nk")
            log.info("Metadata found. Append=%s | New records=%s", append, new_records)
        else:
            df, new_records, append = df, True, append
            log.info("No metadata found. Treating all rows as new; Append=%s", append)

        return df, new_records, append
    except Exception as exc:
        log.exception("read_old_and_new_df failed: %s", exc)
        raise