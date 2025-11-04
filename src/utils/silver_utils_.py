from pyspark.sql import functions as F 
import pyspark 
import json 
from pyspark.sql import types as T 


def non_empty(c):
        return F.col(c).isNotNull() & (F.trim(F.col(c)).cast("string") != "")

def cast_and_collect_bad(df, date_cols, float_cols, date_fmt="dd/MM/yyyy", dedup_bad=True):
    """
    Cast date columns -> timestamp and numeric columns -> float.
    Build a bad_df made of rows where any cast failed (NULL after cast while original looked non-empty).
    Returns: good_df (with casts applied), bad_df (concat of all bad rows).
    """
    df2 = df
    bad_parts = []

    

    # Dates -> timestamp
    for c in date_cols:
        if c in df2.columns:
            cast_col = F.to_timestamp(F.col(c), date_fmt)
            # capture bad rows for this column
            bad_c = df2.filter(cast_col.isNull() & non_empty(c)).select(*df.columns)
            bad_parts.append(bad_c)
            # apply the cast
            df2 = df2.withColumn(c, cast_col)

    # Numerics -> float (use try_cast to tolerate malformed input)
    for c in float_cols:
        if c in df2.columns:
            cast_col = F.expr(f"try_cast(`{c}` as float)")
            bad_c = df2.filter(cast_col.isNull() & non_empty(c)).select(*df.columns)
            bad_parts.append(bad_c)
            df2 = df2.withColumn(c, cast_col)

    # concat all bad rows
    if bad_parts:
        bad_df = bad_parts[0]
        for part in bad_parts[1:]:
            bad_df = bad_df.unionByName(part, allowMissingColumns=True)
        if dedup_bad:
            bad_df = bad_df.dropDuplicates()
    else:
        # empty bad_df with same columns
        bad_df = df2.limit(0).select(*df.columns)

    good_df = df2.dropDuplicates()
    return good_df, bad_df


# def add_sales_features(df,
#                        order_col="order_date",
#                        ship_col="ship_date",
#                        ship_mode_col="ship_mode",
#                        customer_name_col="customer_name",
#                        country_col="country",
#                        city_col="city",
#                        date_fmt="dd/MM/yyyy"):
#     """
#     Adds common time, shipping, and entity features.
#     Uses try_cast where possible to avoid job failures on bad data.
#     Spark 4.0+ (withColumns).
#     """

#     # --- Parse dates safely ---
#     updates = {
#         "order_date_ts": F.to_timestamp(F.col(order_col), date_fmt),
#         "ship_date_ts":  F.to_timestamp(F.col(ship_col),  date_fmt),
#     }

#     # --- Order date features ---
#     updates.update({
#         "order_year":        F.year("order_date_ts"),
#         "order_quarter":     F.quarter("order_date_ts"),
#         "order_month":       F.month("order_date_ts"),
#         "order_day":         F.dayofmonth("order_date_ts"),
#         "order_weekofyear":  F.weekofyear("order_date_ts"),
#         "order_day_of_week_num": F.dayofweek("order_date_ts"),  # 1=Sun ... 7=Sat
#         "order_day_of_week": F.date_format("order_date_ts", "EEEE"),
#         "order_is_workday":  ~F.dayofweek("order_date_ts").isin(1,7),  # Mon-Fri True
#         "order_is_month_start": (F.dayofmonth("order_date_ts") == F.lit(1)),
#         "order_is_month_end":   (F.dayofmonth("order_date_ts") == F.dayofmonth(F.last_day("order_date_ts"))),
#         "order_yyyymm":      F.date_format("order_date_ts", "yyyy-MM"),
#     })

#     # --- Ship date features (mirror) ---
#     updates.update({
#         "ship_year":        F.year("ship_date_ts"),
#         "ship_quarter":     F.quarter("ship_date_ts"),
#         "ship_month":       F.month("ship_date_ts"),
#         "ship_day":         F.dayofmonth("ship_date_ts"),
#         "ship_weekofyear":  F.weekofyear("ship_date_ts"),
#         "ship_day_of_week_num": F.dayofweek("ship_date_ts"),
#         "ship_day_of_week": F.date_format("ship_date_ts", "EEEE"),
#         "ship_is_workday":  ~F.dayofweek("ship_date_ts").isin(1,7),
#         "ship_is_month_start": (F.dayofmonth("ship_date_ts") == F.lit(1)),
#         "ship_is_month_end":   (F.dayofmonth("ship_date_ts") == F.dayofmonth(F.last_day("ship_date_ts"))),
#         "ship_yyyymm":      F.date_format("ship_date_ts", "yyyy-MM"),
#     })

#     # --- Cross-date shipping metrics ---
#     updates.update({
#         "days_to_ship":          F.datediff("ship_date_ts", "order_date_ts"),
#         "same_day_shipping":     (F.datediff("ship_date_ts", "order_date_ts") == F.lit(0)),
#         "is_negative_leadtime":  (F.datediff("ship_date_ts", "order_date_ts") < 0),
#         "ship_speed_bucket": F.when(F.col("days_to_ship") <= 1, "express")
#                                .when(F.col("days_to_ship") <= 3, "standard")
#                                .when(F.col("days_to_ship") >  3, "slow")
#                                .otherwise(None),
#     })

#     # --- Ship mode normalization ---
#     if ship_mode_col in df.columns:
#         updates.update({
#             "ship_mode_norm": F.lower(F.trim(F.col(ship_mode_col))),
#         })

#     # --- Customer name quick splits ---
#     if customer_name_col in df.columns:
#         # split by last space; if single token, last name = null
#         parts = F.split(F.col(customer_name_col), r"\s+")
#         updates.update({
#             "customer_first_name": F.element_at(parts, 1),
#             "customer_last_name":  F.when(F.size(parts) > 1, F.element_at(parts, -1)),
#             "customer_name_len":   F.length(F.col(customer_name_col)),
#         })

#     # --- Location composites ---
#     if city_col in df.columns and country_col in df.columns:
#         updates.update({
#             "city_country": F.concat_ws(", ", F.col(city_col), F.col(country_col)),
#         })
#         # "order_date_ts": F.to_timestamp(F.col(order_col), date_fmt),
#         # "ship_date_ts"
#     col_to_drop = ["order_date","ship_date"]
#     # Apply everything in one logical step
#     enriched = (
#             df.withColumns(updates).drop(*col_to_drop).withColumnsRenamed({
#               "order_date_ts": "order_date",
#               "ship_date_ts" : "shipment_date",
#               "ship_mode": "shipment_mode",
#                 "segment": "customer_segment"
#           })
#             )
#     return enriched




def add_sales_features(
    df,
    order_col="order_date",
    ship_col="ship_date",
    ship_mode_col="ship_mode",
    customer_name_col="customer_name",
    country_col="country",
    city_col="city",
    date_fmt="dd/MM/yyyy",
):
    # 1) Parse dates (FIRST pass)
    parsed = df.withColumns({
        "order_date_ts": F.to_timestamp(F.col(order_col), date_fmt),
        "ship_date_ts":  F.to_timestamp(F.col(ship_col),  date_fmt),
    })

    # 2) Derive order-date features (SECOND pass)
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

    # 3) Ship-date features (THIRD pass)
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

    # 4) Cross-date metrics (FOURTH pass)
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

    #Optional normalizations/splits
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

    # 6) Drop/rename at the end 
    to_drop = [c for c in [order_col, ship_col] if c in out.columns]
    out = out.drop(*to_drop)

    # Use parameterized renames where possible
    rename_map = {
        "order_date_ts": "order_date",
        "ship_date_ts":  "shipment_date",
    }
    
    rename_map = {k: v for k, v in rename_map.items() if k in out.columns}
    out = out.withColumnsRenamed(rename_map)

    
    if "ship_mode" in out.columns:
        out = out.withColumnRenamed("ship_mode", "shipment_mode")
    if "segment" in out.columns:
        out = out.withColumnRenamed("segment", "customer_segment")

    return out


def get_df_schema(df:pyspark.sql.DataFrame,columns_to_drop):
    df = df.drop(*columns_to_drop)
    return df.schema

def schema_json_to_struct(schema_json):
    schema_json = json.loads(schema_json)
    schema_struct = T.StructType.fromJson(schema_json)
    return schema_struct


    
def bronze_column_clean_enrich_schema(df, date_cols, float_cols,date_fmt="dd/MM/yyyy", dedup_bad=True):
    good_df, bad_df = cast_and_collect_bad(df, date_cols=date_cols, float_cols=float_cols,date_fmt="dd/MM/yyyy", dedup_bad=True)
    df = add_sales_features(good_df)
    silver_schema = get_df_schema(df,columns_to_drop=["row_id"])
    return df, bad_df,silver_schema


