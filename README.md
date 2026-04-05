# NYC Taxi Big Data Pipeline

**Course:** Big Data Engineering
**Lecturer:** Harttu Toivonen
**Date:** April 2026

## 1. Architecture and Pipeline Description
**Architecture Flow:** `Raw Data → Apache Spark → MinIO (S3) → DuckDB → FastAPI`

This project implements a complete end-to-end data engineering pipeline. Raw NYC TLC trip data (in Parquet format) is initially ingested into a MinIO object storage S3 bucket. Apache Spark connects to this local S3 bucket using the S3A connector to process the files. During the processing phase, Spark filters out invalid records (e.g., rides with zero passengers or negative trip distances) and performs feature engineering to extract the hour of the pickup. Finally, a FastAPI application utilizes DuckDB to query the processed Parquet files directly from S3, serving the analytical results via REST API endpoints.

## 2. Data Lake Structure
The storage layer utilizes a Medallion Data Lake architecture organized logically within MinIO. All data is stored in the highly compressed, columnar Parquet format.

The main bucket, `taxi-data`, contains three distinct layers:
* **`/raw` (Bronze):** Contains the untouched, raw NYC Yellow Taxi trip records downloaded directly from the TLC website.
* **`/silver`:** Contains the cleaned data. Records with `passenger_count <= 0` or `trip_distance <= 0` have been purged to ensure data quality.
* **`/gold`:** Contains the aggregated data built for specific business analytics, such as the grouped hourly demand calculations.

## 3. Data Analysis and Insights
A FastAPI server was deployed to expose the processed data. The interactive documentation is available at the `/docs` route. Below is an interpretation of the results generated from the three primary API endpoints.

* **Market Share & Revenue:** Based on the data from the Silver layer, Vendor 2 handles a massive 1,802,925 total rides with an average fare of $18.81. Vendor 1 handles significantly fewer rides at 492,595, but maintains a higher average fare of $20.59. Vendor 2 dominates the market share in terms of volume, but Vendor 1 manages to command a higher average revenue per trip.
* **Hourly Demand:** By querying the aggregated Gold layer, the absolute peak hour for taxi pickups is 18:00 (6:00 PM), which recorded 170,329 rides. Conversely, the lowest demand occurs at 04:00 (4:00 AM) with only 11,722 rides. The surge in demand strongly correlates with the evening commute and the end of the standard workday.
* **Passenger Influence:** Single-passenger rides average a distance of 3.31 miles. Rides with larger groups maintain remarkably similar averages; for example, 4-passenger trips average 4.14 miles, and 6-passenger trips average 3.26 miles. The data indicates that average trip distance remains relatively consistent regardless of party size.

## 4. How to Run the Project
1. **Start MinIO:** Start the local MinIO server on port 9000/9001.
2. **Process Data:** Run `python process_data.py` to trigger the Spark job, which cleans and aggregates the S3 data.
3. **Start API:** Run `uvicorn api:app --reload --port 8080` to start the FastAPI server.
4. Navigate to `http://localhost:8080/docs` to view the interactive Swagger documentation and query the Data Lake.