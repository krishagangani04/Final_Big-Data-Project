from fastapi import FastAPI
import duckdb

app = FastAPI(
    title="NYC Taxi Big Data API",
    description="API for querying processed NYC TLC taxi data."
)

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("""
    SET s3_region='us-east-1';
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

@app.get("/")
def home():
    return {"message": "Welcome to the NYC Taxi API. Go to /docs for the interactive documentation."}

@app.get("/demand-by-hour")
def get_hourly_demand():

    query = "SELECT * FROM 's3://taxi-data/gold/hourly_demand.parquet/*.parquet' ORDER BY hour"
    df = con.execute(query).df()
    return df.to_dict(orient="records")

@app.get("/provider-summary")
def get_provider_summary():

    query = """
        SELECT VendorID, COUNT(*) as total_rides, AVG(fare_amount) as avg_fare 
        FROM 's3://taxi-data/silver/cleaned_taxi_data.parquet/*.parquet' 
        GROUP BY VendorID
    """
    df = con.execute(query).df()
    return df.to_dict(orient="records")

@app.get("/passenger-stats")
def get_passenger_stats():

    query = """
        SELECT passenger_count, AVG(trip_distance) as avg_distance 
        FROM 's3://taxi-data/silver/cleaned_taxi_data.parquet/*.parquet' 
        GROUP BY passenger_count 
        ORDER BY passenger_count
    """
    df = con.execute(query).df()
    return df.to_dict(orient="records")