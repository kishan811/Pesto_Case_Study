from pyspark.sql import SparkSession

class TaskManager:
    def __init__(self):
        self.tasks = []

    def add_task(self, task):
        self.tasks.append(task)

    def update_task_status(self, task_id, new_status):
        for task in self.tasks:
            if task['id'] == task_id:
                task['status'] = new_status
                break
        else:
            print("Task not found.")

    def delete_task(self, task_id):
        for idx, task in enumerate(self.tasks):
            if task['id'] == task_id:
                del self.tasks[idx]
                break
        else:
            print("Task not found.")

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

# Create a TaskManager instance
task_manager = TaskManager()

# Add tasks
task_manager.add_task({"id": 1, "description": "Sample task 1", "status": "Pending"})
task_manager.add_task({"id": 2, "description": "Sample task 2", "status": "Pending"})

# Update task status
task_manager.update_task_status(1, "Completed")

# Delete task
task_manager.delete_task(2)

# Stop SparkSession
spark.stop()
