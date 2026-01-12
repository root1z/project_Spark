import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkConf
from schemas.event_schema import schema
from transformation.product_view import build_ready_df, build_final_df
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME, KAFKA_CONFIG
from utils.logger import setup_logger
from writers.postgres_writer import write_to_postgres_spark_jdbc
import os

def run():
    spark_conf_dict = {
        "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.6.0",
        "spark.app.name": "KafkaSparkPostgres",
        "spark.executor.memory": "512m",
        "spark.executor.cores": "1",
        "spark.executor.instances": "2",
        "spark.driver.memory": "512m",
        "spark.sql.shuffle.partitions": "4",
        "spark.yarn.executor.memoryOverhead": "512m"
    }
    spark_conf = SparkConf().setAll(spark_conf_dict.items())
    security_protocol = KAFKA_CONFIG.get("security.protocol")
    sasl_mechanism = KAFKA_CONFIG.get("sasl.mechanism")
    kafka_username = KAFKA_CONFIG.get("sasl.username")
    kafka_password = KAFKA_CONFIG.get("sasl.password")
    kafka_conf = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "subscribe": TOPIC_NAME['data_output'],
        "startingOffsets": "earliest",
        "failOnDataLoss": "false"
    }
    if security_protocol != "PLAINTEXT":
        kafka_conf["kafka.security.protocol"] = security_protocol
        kafka_conf["kafka.sasl.mechanism"] = sasl_mechanism
        kafka_conf["kafka.sasl.jaas.config"] = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";'
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    logger = setup_logger("spark_streaming")
    logger.info(f"spark_conf: {spark_conf.getAll()}")
    safe_kafka_conf = dict(kafka_conf)
    if "kafka.sasl.jaas.config" in safe_kafka_conf:
        safe_kafka_conf["kafka.sasl.jaas.config"] = "***"
    logger.info(f"kafka_conf: {safe_kafka_conf}")
    df = spark.readStream.format("kafka").options(**kafka_conf).load()
    parsed_df = df.select(f.from_json(f.col("value").cast("string"), schema).alias("data")).select("data.*")
    ready_df = build_ready_df(parsed_df)
    final_df = build_final_df(ready_df)
    writer = write_to_postgres_spark_jdbc
    checkpoint_path = "./checkpoint/spark_jdbc/product_streaming"
    query = final_df.writeStream.foreachBatch(writer).outputMode("append").option("checkpointLocation", checkpoint_path).trigger(processingTime="30 seconds").start()
    query.awaitTermination()
