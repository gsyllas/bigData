import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf, lpad
from pyspark.sql.types import DoubleType
from geopy.distance import geodesic

# Function to calculate distance using geopy
def get_distance(lat1, lon1, lat2, lon2):
    try:
        return geodesic((lat1, lon1), (lat2, lon2)).km
    except ValueError:
        return None

# Register UDF
calculate_distance = udf(get_distance, DoubleType())

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Q4 DF") \
    .getOrCreate()

# Start time tracking
start_time = time.time()

# Paths to the CSV files
crime_data_path_1 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2020_to_Present.csv"
crime_data_path_2 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2010_to_2019.csv"
police_stations_path = "hdfs://master:9000/home/user/ergasia_data/LA_Police_Stations.csv"

# Read datasets into DataFrames
crime_df_1 = spark.read.option("header", "true").csv(crime_data_path_1)
crime_df_2 = spark.read.option("header", "true").csv(crime_data_path_2)
police_df = spark.read.option("header", "true").csv(police_stations_path)

# Union the two crime dataframes
crime_df = crime_df_1.union(crime_df_2)

# Filter out records that refer to Null Island and ensure LAT and LON can be converted to double
crime_df = crime_df.filter((col("LAT") != 0.0) & (col("LON") != 0.0))
crime_df = crime_df.filter(col("LAT").cast(DoubleType()).isNotNull() & col("LON").cast(DoubleType()).isNotNull())

# Filter for firearm crimes and select necessary columns: AREA, LAT, LON, Weapon Used Cd
firearm_crimes_df = crime_df.filter(col("Weapon Used Cd").startswith("1")).select("AREA", "LAT", "LON", "Weapon Used Cd")

# Select necessary columns from police data: PRECINCT, x, y, DIVISION
police_df = police_df.withColumnRenamed("PREC", "AREA").select("AREA", "x", "y", "DIVISION")

# Standardize AREA codes to the same length
firearm_crimes_df = firearm_crimes_df.withColumn("AREA", lpad(col("AREA"), 2, '0'))
police_df = police_df.withColumn("AREA", lpad(col("AREA"), 2, '0'))

# Convert columns to appropriate types
firearm_crimes_df = firearm_crimes_df.withColumn("LAT", col("LAT").cast("double")).withColumn("LON", col("LON").cast("double"))
police_df = police_df.withColumn("x", col("x").cast("double")).withColumn("y", col("y").cast("double"))

#  Join
police_df = police_df.alias("p")
joined_df_ = firearm_crimes_df.alias("c") \
    .join(police_df, col("c.AREA") == col("p.AREA"), "inner")

# Calculate distances
joined_df_ = joined_df_.withColumn(
    "distance", calculate_distance(col("c.LAT"), col("c.LON"), col("p.y"), col("p.x"))
)

# Aggregate results
agg_results = joined_df_.groupBy("p.DIVISION").agg(
    expr("count(*) as crime_count"),
    expr("avg(distance) as avg_distance")
).orderBy(col("crime_count").desc())

# Collect and display results
print(" Join Results:")
agg_results.show(100)

# End time tracking
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed time: {elapsed_time} seconds")

# Stop SparkSession
spark.stop()

