from pyspark.sql import SparkSession
import pyspark
from utils.bronze_utils import check_metadata_in_folder, get_df_not_present
from utils.general_utils import build_spark, get_logger

log  = get_logger()
spark = build_spark(local=True)
def read_gold_df(silver_data_path:str,spark:pyspark.sql.SparkSession):
    gold_df = spark.read.parquet(silver_data_path)
    return gold_df 

def split_data_sales_cust(gold_df, cust_cols, sales_cols):
    """
    Splits a single DataFrame into two:
      - sales_df: columns listed in sales_cols
      - cust_df:  columns listed in cust_cols

    Returns:
        (sales_df, cust_df)
    """
    sales_df = gold_df.select(*sales_cols)
    cust_df = gold_df.select(*cust_cols)
    return sales_df, cust_df
    
def read_old_and_new_df(metadata_folder_path, metadata_file_name,path_to_old_df,df):
    new_df = df
    append = False
    if check_metadata_in_folder(metadata_folder_path=metadata_folder_path, metadata_file_name=metadata_file_name):
        append = True
        df, new_records,append= get_df_not_present(path_to_old_df=path_to_old_df, new_df=new_df,uniq_column="nk") 
    else:
        df, new_records,append = df, True,append

    return df, new_records,append