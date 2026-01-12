from pyspark.sql.types import StringType, StructType, StructField, DoubleType, BooleanType, ArrayType

schema = StructType([
    StructField("_id", StringType(), True),
    StructField("time_stamp", DoubleType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("resolution", StringType(), True),
    StructField("user_id_db", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("api_version", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("local_time", StringType(), True),
    StructField("show_recommendation", StringType(), True),
    StructField("current_url", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("recommendation", BooleanType(), True),
    StructField("utm_source", StringType(), True),
    StructField("utm_medium", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("option", ArrayType(
        StructType([
            StructField("option_label", StringType(), True),
            StructField("option_id", StringType(), True),
            StructField("value_label", StringType(), True),
            StructField("value_id", StringType(), True)
        ])
    ), True),
    StructField("id", StringType(), True)
])
