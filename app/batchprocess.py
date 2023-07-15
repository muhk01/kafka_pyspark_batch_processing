try:

    import os
    import sys
    import uuid
    import json

    import pyspark
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    from pyspark.sql.functions import col, asc, desc
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark import SparkContext, SparkConf
    from pyspark.streaming import StreamingContext

    import findspark

    print("ok.....")
except Exception as e:
    print("Error : {} ".format(e))

BOOT_STRAP = "localhost:9092"
TOPIC = 'messages'

spark = SparkSession.builder \
    .appName("kafka-example") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOT_STRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("includeHeaders", "true") \
    .option("failOnDataLoss", "false") \
    .load()

def process_batch_message(df, batch_id):
    my_df = df.selectExpr("CAST(value AS STRING) as json")
    schema = StructType(
        [
            StructField("user_id",StringType(),True),
            StructField("recipient_id",StringType(),True),
            StructField("message",StringType(),True),
        ]
    )
    clean_df = my_df.select(from_json(col("json").cast("string"), schema).alias("data")).select("data.*")
    
    print("batch_id : ", batch_id, clean_df.show(truncate=False))

query = kafka_df.writeStream \
    .foreachBatch(process_batch_message) \
    .trigger(processingTime="1 minutes") \
    .start().awaitTermination()
