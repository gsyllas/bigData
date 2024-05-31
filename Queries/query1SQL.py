import time
from pyspark.sql import SparkSession

# Start time tracking
start_time = time.time()

spark = SparkSession.builder \
    .appName("CrimeStatistics") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Paths to the CSV files
csv_file_path_1 = "C:/Users/gsyllas/Dsml/BigData/Crime_Data_from_2010_to_2019.csv"
csv_file_path_2 = "C:/Users/gsyllas/Dsml/BigData/Crime_Data_from_2020_to_Present.csv"

# Load the datasets
df1 = spark.read.option("header", "true").csv(csv_file_path_1)
df2 = spark.read.option("header", "true").csv(csv_file_path_2)

# Create temporary views
df1.createOrReplaceTempView("crime_data_2010_2019")
df2.createOrReplaceTempView("crime_data_2020_present")

# Define SQL query to convert date and extract year and month
sql_query = """
    SELECT 
        to_date(`Date Rptd`, 'M/d/yyyy H:mm') AS Date_Rptd,
        year(to_date(`Date Rptd`, 'M/d/yyyy H:mm')) AS Year,
        month(to_date(`Date Rptd`, 'M/d/yyyy H:mm')) AS Month
    FROM crime_data_2010_2019
    UNION ALL
    SELECT 
        to_date(`Date Rptd`, 'M/d/yyyy H:mm') AS Date_Rptd,
        year(to_date(`Date Rptd`, 'M/d/yyyy H:mm')) AS Year,
        month(to_date(`Date Rptd`, 'M/d/yyyy H:mm')) AS Month
    FROM crime_data_2020_present
"""

# Create a temporary view for the unified data
spark.sql(sql_query).createOrReplaceTempView("unified_crime_data")

# Group by year and month and count the number of crimes
grouped_query = """
    SELECT 
        Year, 
        Month, 
        COUNT(*) AS count
    FROM unified_crime_data
    GROUP BY Year, Month
"""

# Create a temporary view for the grouped data
spark.sql(grouped_query).createOrReplaceTempView("monthly_crime_counts")

# Define SQL query to rank the months within each year based on the number of crimes
ranked_query = """
    SELECT
        Year,
        Month,
        count,
        RANK() OVER (PARTITION BY Year ORDER BY count DESC) AS Rank
    FROM monthly_crime_counts
"""

# Create a temporary view for the ranked data
spark.sql(ranked_query).createOrReplaceTempView("ranked_crime_counts")

# Select the top 3 months per year
top3_query = """
    SELECT 
        Year,
        Month,
        count,
        Rank
    FROM ranked_crime_counts
    WHERE Rank <= 3
    ORDER BY Year, count DESC
"""

# Execute the query and show the results
top3_months_per_year = spark.sql(top3_query)
top3_months_per_year.show()

# Stop the SparkSession
spark.stop()

# End time tracking
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Elapsed time: {elapsed_time} seconds")
