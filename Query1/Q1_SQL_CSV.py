import time
from pyspark.sql import SparkSession

# Start time tracking
start_time = time.time()

spark = SparkSession\
	.builder \
	.appName("Q1 SQL CSV files") \
    	.getOrCreate()

# Paths to the CSV files
file_path_1 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2010_to_2019.csv"
file_path_2 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2020_to_Present.csv"

# Load the datasets
df1 = spark.read.option("header", "true").csv(file_path_1)
df2 = spark.read.option("header", "true").csv(file_path_2)

# Create temporary views
df1.createOrReplaceTempView("crime_data_2010_2019")
df2.createOrReplaceTempView("crime_data_2020_present")

# Define SQL query to unify data and keep only 'DATE OCC' columns 
query1 = """
	SELECT `DATE OCC` FROM crime_data_2010_2019
	UNION ALL
	SELECT `DATE OCC` FROM crime_data_2020_present
	"""

spark.sql(query1).createOrReplaceTempView("combined_crime_data")

#Extract year and month from 'DATE OCC'
query2 = """
	SELECT 
        year(to_date(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) as Year, 
        month(to_date(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) as Month
	FROM combined_crime_data
	"""

spark.sql(query2).createOrReplaceTempView("crime_data")


# Group by year and month and count the number of crimes
query3 = """
    SELECT 
        Year, 
        Month, 
        COUNT(*) AS Crime_total
    FROM crime_data
    GROUP BY Year, Month
"""

spark.sql(query3).createOrReplaceTempView("monthly_crime_counts")

#Rank the months within each year based on the number of crimes
query4 = """
    SELECT
        Year,
        Month,
        Crime_total,
        RANK() OVER (PARTITION BY Year ORDER BY Crime_total DESC) AS Rank
    FROM monthly_crime_counts
"""

spark.sql(query4).createOrReplaceTempView("ranked_crime_counts")

# Select the top 3 months per year
query5 = """
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
top3_months_per_year = spark.sql(query5)
top3_months_per_year.show(50)


# End time tracking
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Elapsed time: {elapsed_time} seconds")


# Stop the SparkSession
spark.stop()

