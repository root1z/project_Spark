# Tổng quan dự án
Code nằm trong thư mục `spark/99-project`, được chia thành 2 module chính:

## Module 1 – Streaming Kafka
- Chạy streaming Kafka (file `kafka_streaming.py`)

## Module 2 – Spark Batch
- Chạy Spark batch processing Kafka (file `spark_streaming.py`)
- **Còn thiếu**: phần tạo bảng theo Aggregation

---

# Hướng dẫn chạy code trong thư mục spark/99-project
### chạy streaming Kafka
```bash
source .venv/bin/activate
python3 kafka_streaming.py
```
### Chạy spark batch processing Kafka
- Dừng và xóa container cũ (nếu có)
- Chạy lệnh sau:
```bash
docker container stop test-spark || true && \
docker container rm test-spark || true && \
docker run -ti --name test-spark \
--network=streaming-network \
-v ./:/spark \
-v spark_lib:/home/spark/.ivy2 \
-v spark_data:/data \
-v $(pwd)/hadoop-conf:/spark/hadoop-conf \
-e HADOOP_CONF_DIR=/spark/hadoop-conf \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
-e KAFKA_BOOTSTRAP_SERVERS='kafka-0:9092,kafka-1:9092,kafka-2:9092' \
-e POSTGRES_HOST='postgres' \
-e LOG_LEVEL=DEBUG \
-e LOG_TO_CONSOLE=true \
unigap/spark:3.5 bash -c "(cd /spark/99-project && zip -r /tmp/project.zip config utils browser schemas jobs transformation writers) && \
conda env create --file /spark/environment.yml && \
source ~/miniconda3/bin/activate && \
conda activate pyspark_conda_env && \
conda pack -f -o pyspark_conda_env.tar.gz && \
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.6.0 \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/project.zip \
--deploy-mode client \
--master yarn \
/spark/99-project/spark_streaming.py"
```
