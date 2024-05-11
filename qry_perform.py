from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Data Storage and Query Performance") \
    .getOrCreate()

# Read correlated data
correlated_df = spark.read.parquet("correlated_data.parquet")

# Perform analytics and optimizations
campaign_performance_df = correlated_df.groupBy("campaign_id").count()

# Show campaign performance
campaign_performance_df.show()

# Additional optimizations can be performed here, such as partitioning and indexing

# Stop SparkSession
spark.stop()
