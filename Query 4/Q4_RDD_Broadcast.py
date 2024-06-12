import time
from pyspark import SparkContext, SparkConf, Broadcast
from geopy.distance import geodesic
import csv
from io import StringIO

# Function to calculate distance using geopy
def get_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).km

# Helper function to parse CSV lines
def parse_csv_line(line):
    input = StringIO(line)
    reader = csv.reader(input)
    return next(reader)

# Function to pad area codes with leading zeros
def pad_area_code(area_code):
    return area_code.zfill(2)

# Initialize SparkContext
conf = SparkConf().setAppName("Q4 Broadcast")
sc = SparkContext(conf=conf)

# Start time tracking
start_time = time.time()

# Paths to the CSV files
crime_data_path_1 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2020_to_Present.csv"
crime_data_path_2 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2010_to_2019.csv"
police_stations_path = "hdfs://master:9000/home/user/ergasia_data/LA_Police_Stations.csv"

# Read and parse crime data
crime_rdd_1 = sc.textFile(crime_data_path_1).map(parse_csv_line)
crime_rdd_2 = sc.textFile(crime_data_path_2).map(parse_csv_line)
police_rdd = sc.textFile(police_stations_path).map(parse_csv_line)

# Filter header rows
crime_header = crime_rdd_1.first()
police_header = police_rdd.first()
crime_rdd_1 = crime_rdd_1.filter(lambda row: row != crime_header)
crime_rdd_2 = crime_rdd_2.filter(lambda row: row != crime_header)
police_rdd = police_rdd.filter(lambda row: row != police_header)

# Union the two crime RDDs
crime_rdd = crime_rdd_1.union(crime_rdd_2)

# Filter out records that refer to Null Island (LAT and LON are both 0.0)
def is_valid_lat_lon(lat, lon):
    try:
        return float(lat) != 0.0 and float(lon) != 0.0
    except ValueError:
        return False

crime_rdd = crime_rdd.filter(lambda row: is_valid_lat_lon(row[26], row[27]))

# Filter for firearm crimes and select necessary columns: AREA, LAT, LON
firearm_crimes_rdd = crime_rdd.filter(lambda row: row[16].startswith("1")).map(
    lambda row: (pad_area_code(row[4]), row[26], row[27]))

# Select necessary columns from police data: PRECINCT, x, y, DIVISION
police_rdd = police_rdd.map(lambda row: (pad_area_code(row[5]), (row[0], row[1], row[3])))


# Convert police RDD to a dictionary and broadcast it
police_dict = police_rdd.collectAsMap()
broadcast_police = sc.broadcast(police_dict)

# Custom map function to join crime data with broadcast police data and calculate distances
def join_and_calculate_distance(record):
    area, lat, lon = record
    results = []
    if area in broadcast_police.value:
        police_record = broadcast_police.value[area]
        police_lat, police_lon, division = float(police_record[1]), float(police_record[0]), police_record[2]
        distance = get_distance(float(lat), float(lon), police_lat, police_lon)
        results.append((division, (1, distance)))  # Using DIVISION as the key for aggregation
    return results

# Apply map function to join and calculate distances
joined_rdd = firearm_crimes_rdd.flatMap(join_and_calculate_distance)

# Aggregate results
agg_results = joined_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
final_results = agg_results.mapValues(lambda v: (v[0], v[1] / v[0])).sortBy(lambda x: -x[1][0])

# Collect and display results
print("Broadcast Join Results:")
for result in final_results.collect():
    print(result)

# End time tracking
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed time: {elapsed_time} seconds")

# Stop SparkContext
sc.stop()
