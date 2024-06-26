# Pesto_Case_Study

# Sample Datasets and Python Scripts for AdvertiseX

This repository contains sample datasets and Python scripts for data processing tasks related to digital advertising technology.

## Sample Datasets

### Ad Impressions (JSON format)

File: ad_impressions.json

This JSON file contains sample data representing ad impressions generated by AdvertiseX. Each record includes information such as ad creative ID, user ID, timestamp, and the website where the ad was displayed.

### Clicks and Conversions (CSV format)

File: click_conversion.csv

This CSV file contains sample data representing clicks and conversions tracked by AdvertiseX. Each record includes event timestamp, user ID, campaign ID, and conversion type (e.g., signup, purchase).

### Bid Requests (Avro format)

File: bid_request.avro

This Avro file contains sample data representing bid requests received by AdvertiseX for real-time bidding auctions. Each record includes user ID, auction ID, and ad targeting criteria such as location and interests.

## Python Scripts

### Data Ingestion Script (data_ingest.py)

This Python script demonstrates data ingestion using Apache Spark for processing ad impressions (JSON), clicks/conversions (CSV), and bid requests (Avro) data.

### Data Processing Script (data_process.py)

This Python script demonstrates data processing using Apache Spark for correlating ad impressions with clicks and conversions, and performing additional transformations.

### Data Storage and Query Performance Script (qry_perform.py)

This Python script demonstrates storing processed data efficiently and optimizing query performance using Apache Spark.

### Error Handling and Monitoring Script (error_handling.py)

This Python script demonstrates error handling and monitoring using Apache Spark for detecting anomalies, discrepancies, or delays in the data processing pipeline.

Feel free to use these datasets and scripts for educational purposes, testing data engineering solutions, or any other relevant applications.

