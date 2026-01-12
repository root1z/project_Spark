import pyspark.sql.functions as f
from pyspark.sql.functions import col, to_json

def build_ready_df(parsed_df):
    return parsed_df \
        .withColumn("event_time", (col("time_stamp") / 1000).cast("timestamp")) \
        .withColumn("option_json", to_json(col("option"))) \
        .drop("option", "time_stamp") \
        .withColumnRenamed("_id", "original_id")

def build_final_df(ready_df):
    return ready_df.select(
        col("original_id"),
        col("event_time"),
        col("product_id"),
        col("store_id"),
        col("user_id_db"),
        col("device_id"),
        col("ip"),
        col("user_agent"),
        col("resolution"),
        col("api_version"),
        col("local_time"),
        col("show_recommendation"),
        col("current_url"),
        col("referrer_url"),
        col("email_address"),
        col("recommendation"),
        col("utm_source"),
        col("utm_medium"),
        col("collection"),
        col("option_json")
    )
