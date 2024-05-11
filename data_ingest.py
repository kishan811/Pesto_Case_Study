from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Data Ingestion") \
    .getOrCreate()

# Read JSON, CSV, and Avro data into Spark DataFrames
ad_impressions_df = spark.read.json("ad_impressions.json")
clicks_conversions_df = spark.read.csv("clicks_conversions.csv", header=True)
bid_requests_df = spark.read.format("avro").load("bid_requests.avro")

# Show sample data
ad_impressions_df.show()
clicks_conversions_df.show()
bid_requests_df.show()

# Additional processing steps can be added here, such as data validation and cleaning

# Stop SparkSession
spark.stop()
