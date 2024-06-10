from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
import time

start = time.time()
# Initialize Spark session
spark = SparkSession\
	.builder\
	.appName("Q1 DataFrame Parquet files")\
	.getOrCreate()

# Load the parquet files into a Spark DataFrame and keep only "DATE OCC" column
file_path = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_parquet"
df = spark.read.parquet(file_path).select("DATE OCC")

# Convert 'DATE OCC' to date type and extract year and month
df = df.withColumn("DATE OCC", to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a")) \
       .withColumn("Year", year(col("DATE OCC"))) \
       .withColumn("Month", month(col("DATE OCC")))


# Group by year and month, then count the number of incidents
monthly_crime_counts = df.groupBy("Year", "Month").count().withColumnRenamed("count", "Crime Total")

# Define window specification to rank months within each year
window_spec = Window.partitionBy("Year").orderBy(desc("Crime Total"))

# Apply window function to get the rank
ranked_df = monthly_crime_counts.withColumn("Rank", rank().over(window_spec))

# Filter top 3 months for each year
top_3_months_per_year = ranked_df.filter(col("Rank") <= 3)

# Sort by year (ascending) and incident count (descending)
sorted_df = top_3_months_per_year.orderBy("Year", desc("Crime Total"))

# Show the results
sorted_df.select("Year", "Month", "Crime Total", "Rank").show(50)

#calculate and print elapsed time
end = time.time()
elapsed_time = end - start
print(f"Elapsed time: {elapsed_time} seconds")

#Stop spark session
spark.stop()
