from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, BinaryType
from pymongo import MongoClient
import os

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.7'

# Define MongoDB connection configuration
mongoUri = "mongodb://localhost:27017/video_data"
mongoCollection = "video_info"

# Create PyMongo client and get reference to video collection
mongoClient = MongoClient(mongoUri)
mongoDb = mongoClient.get_database()
mongoVideoCollection = mongoDb[mongoCollection]

spark = SparkSession.builder \
    .appName("VideoAnalysis") \
    .config("spark.executor.extraJavaOptions", "-Dkafka.consumer.thread.interruptible=false") \
    .getOrCreate()

# Define Kafka consumer configuration
kafkaParams = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "krishna",
    "startingOffsets": "earliest"
}

# Define schema for video data
videoSchema = StructType([StructField("video", BinaryType(), True)])
# Create empty DataFrame with video schema and write to 'video' table
spark.createDataFrame([], videoSchema).write.mode("overwrite").saveAsTable("video")

# Define function to process video data and write results to MongoDB
def process_video(df, epoch_id):
    if df is not None and not df.rdd.isEmpty():
        video_data = []
        for row in df.rdd.collect():
            if row.video is not None:
                video_data.extend(row.video)
        if len(video_data) > 0:
            # Process video data here
            print(f"Processed {len(video_data)} bytes of video data")
            # Register DataFrame as temporary view for Spark SQL queries
            df.createOrReplaceTempView("video")
            # Example Spark SQL query: count number of bytes in each video chunk
            byteCount = spark.sql("SELECT LENGTH(video) as byte_count FROM video")
            byteCount.show()
            # Example Spark SQL query: calculate average byte count per video chunk
            avgByteCount = spark.sql("SELECT AVG(LENGTH(video)) as avg_byte_count FROM video")
            avgByteCount.show()
            # Example Spark SQL query: insert video data into 'video' table
            df.write \
              .format("parquet") \
              .mode("append") \
              .insertInto("video")
            # Write processed video data to MongoDB
            mongoVideoCollection.insert_many(df.collect())
        else:
            print("No video data received in this batch")

# Read video data from Kafka using Spark SQL Kafka 0-10
videoStream = spark \
    .readStream \
    .format("kafka") \
    .options(**kafkaParams) \
    .load() \
    .selectExpr("CAST(value AS BINARY)") \
    .select(from_json(col("value").cast("string"), videoSchema).alias("data")) \
    .select("data.*")

# Process video stream using process_video function
query = videoStream.writeStream \
    .foreachBatch(process_video) \
    .start()

query.awaitTermination()



