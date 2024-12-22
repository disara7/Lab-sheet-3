from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka-Spark-Streaming") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Kafka Configuration
kafka_broker = "kafka:9092"
kafka_topic = "test-topic"

# Read Kafka Stream
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .load()

# Schema for Kafka messages
schema = StructType().add("message", StringType())

# Parse and transform Kafka stream
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.message")

transformed_stream = parsed_stream.withColumn(
    "word_count", col("message").substr(1, 100)
)

# Output transformed stream to console
query = transformed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
