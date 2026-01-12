import pyspark.sql.functions as f
from config.postgres_config import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME
from utils.logger import setup_logger
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"


def _to_int_or_hash(c):
    return f.when(c.cast("int").isNotNull(), c.cast("int")).otherwise(f.abs(f.hash(c)))

def build_dimensions(batch_df):
    product_id_int = _to_int_or_hash(f.col("product_id"))
    store_id_int = _to_int_or_hash(f.col("store_id"))
    user_id_int = _to_int_or_hash(f.col("user_id_db"))
    device_id_int = _to_int_or_hash(f.col("device_id"))
    date_df = batch_df.select(
        f.to_date(f.col("event_time")).alias("full_date"),
        f.hour(f.col("event_time")).alias("hour"),
        f.dayofmonth(f.col("event_time")).alias("day"),
        f.month(f.col("event_time")).alias("month"),
        f.year(f.col("event_time")).alias("year"),
        f.dayofweek(f.col("event_time")).alias("dow")
    ).withColumn("is_weekend", f.col("dow").isin(1, 7)) \
     .withColumn("date_id", (f.col("year") * f.lit(10000) + f.col("month") * f.lit(100) + f.col("day")).cast("int")) \
     .drop("dow").dropDuplicates(["date_id"])
    products_df = batch_df.select(product_id_int.alias("product_id"), f.lit(None).cast("string").alias("product_name")).dropDuplicates(["product_id"]).filter(f.col("product_id").isNotNull())
    stores_df = batch_df.select(store_id_int.alias("store_id"), f.lit(None).cast("string").alias("store_name")).dropDuplicates(["store_id"]).filter(f.col("store_id").isNotNull())
    users_df = batch_df.select(user_id_int.alias("user_id"), f.col("email_address").alias("email_address")).dropDuplicates(["user_id"]).filter(f.col("user_id").isNotNull())
    devices_df = batch_df.select(device_id_int.alias("device_id"), f.col("device_id").alias("raw_device_id"), f.col("resolution").alias("resolution"), f.col("user_agent").alias("user_agent"), f.lit(None).cast("string").alias("os"), f.lit(None).cast("string").alias("device_model"), f.lit(None).cast("string").alias("device_type")).dropDuplicates(["device_id"]).filter(f.col("device_id").isNotNull())
    return {
        "dim_product": products_df,
        "dim_store": stores_df,
        "dim_user": users_df,
        "dim_device": devices_df,
        "dim_date": date_df,
        "ids": {
            "product_id": product_id_int,
            "store_id": store_id_int,
            "user_id": user_id_int,
            "device_id": device_id_int
        }
    }

def build_fact(batch_df, ids):
    return batch_df.select(
        f.abs(f.hash(f.coalesce(f.col("original_id"), f.concat_ws("-", f.col("ip"), f.col("event_time").cast("string"))))).alias("view_id"),
        ids["product_id"].alias("product_id"),
        ids["device_id"].alias("device_id"),
        ((f.year(f.col("event_time")) * f.lit(10000) + f.month(f.col("event_time")) * f.lit(100) + f.dayofmonth(f.col("event_time"))).cast("int")).alias("date_id"),
        ids["store_id"].alias("store_id"),
        ids["user_id"].alias("user_id"),
        f.lit(None).cast("int").alias("location_id"),
        f.col("ip").alias("ip_address"),
        f.col("api_version").alias("api_version"),
        f.col("referrer_url").alias("referrer_url"),
        (f.col("event_time").cast("timestamp").cast("long") * f.lit(1000)).alias("time_stamp")
    )

def _write_df_jdbc(df, table):
    df.write.mode("append").format("jdbc").option("url", JDBC_URL).option("dbtable", table).option("user", DB_USER).option("password", DB_PASS).option("driver", "org.postgresql.Driver").save()

def write_to_postgres_spark_jdbc(batch_df, batch_id):
    logger = setup_logger("write_to_postgres_spark_jdbc")
    if batch_df.rdd.isEmpty():
        logger.info(f"Batch {batch_id} is empty")
        return
    dims = build_dimensions(batch_df)
    fact_df = build_fact(batch_df, dims["ids"])
    try:
        _write_df_jdbc(dims["dim_product"], "dim_product")
        _write_df_jdbc(dims["dim_store"], "dim_store")
        _write_df_jdbc(dims["dim_user"], "dim_user")
        _write_df_jdbc(dims["dim_device"], "dim_device")
        _write_df_jdbc(dims["dim_date"], "dim_date")
        _write_df_jdbc(fact_df, "fact_view")
    except Exception as e:
        logger.error(str(e))
        raise e
