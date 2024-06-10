import time
from pyspark.sql import SparkSession

# Start time tracking
start_time = time.time()

spark = SparkSession\
	.builder \
	.appName("Q1 SQL Parquet files") \
    	.getOrCreate()

# Paths to Parquet files
file_path = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_parquet"

# Load the datasets
df = spark.read.parquet(file_path)

# Create temporary views
df.createOrReplaceTempView("crime_data")


#Extract year and month from 'DATE OCC'
query1 = """
	SELECT 
        year(to_date(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) as Year, 
        month(to_date(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) as Month
	FROM crime_data
	"""

spark.sql(query1).createOrReplaceTempView("crime_data")


# Group by year and month and count the number of crimes
query2 = """
    SELECT 
        Year, 
        Month, 
        COUNT(*) AS Crime_total
    FROM crime_data
    GROUP BY Year, Month
"""

spark.sql(query2).createOrReplaceTempView("monthly_crime_counts")

#Rank the months within each year based on the number of crimes
query3 = """
    SELECT
        Year,
        Month,
        Crime_total,
        RANK() OVER (PARTITION BY Year ORDER BY Crime_total DESC) AS Rank
    FROM monthly_crime_counts
"""

spark.sql(query3).createOrReplaceTempView("ranked_crime_counts")

# Select the top 3 months per year
query4 = """
    SELECT 
        Year,
        Month,
        Crime_total,
        Rank
    FROM ranked_crime_counts
    WHERE Rank <= 3
    ORDER BY Year, Crime_total DESC
"""

# Execute the query and show the results
top3_months_per_year = spark.sql(query4)
top3_months_per_year.show(50)


# End time tracking
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Elapsed time: {elapsed_time} seconds")


# Stop the SparkSession
spark.stop()

