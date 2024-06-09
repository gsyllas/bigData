from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
import time

start = time.time()
# Initialize Spark session
spark = SparkSession\
	.builder\
	.appName("Q1 DataFrame CSV files")\
	.getOrCreate()

# Load the CSV file into a Spark DataFrame
file_path1 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2010_to_2019.csv"
file_path2 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2020_to_Present.csv"
df1 = spark.read.option("header", "true").csv(file_path1)
df2 = spark.read.option("header", "true").csv(file_path2)

#Keep only 'DATE OCC' column and Combine the two datafames
df1 = df1.select("DATE OCC")
df2 = df2.select("DATE OCC")
df = df1.union(df2)

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
