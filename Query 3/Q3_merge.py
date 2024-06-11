from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, trim, split, length, desc, regexp_replace, count
from pyspark.sql.types import IntegerType
import time

# Start time measurement
start = time.time()

# Initialize Spark session
spark = SparkSession\
	.builder\
	.appName("Q3 merge")\
	.getOrCreate()
spark.catalog.clearCache()

# Load the datasets
crime_path = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_parquet"
geocode_path = "hdfs://master:9000/home/user/ergasia_data/revgecoding.csv"
income_path = "hdfs://master:9000/home/user/ergasia_data/LA_income_2015.csv"

crime_df = spark.read.parquet(crime_path)
geocode_df = spark.read.option("header", "true").csv(geocode_path)
income_df = spark.read.option("header", "true").csv(income_path)

#Keep necessary columns from crime_df
crime_df = crime_df.select("DATE OCC", "Vict Descent", "LAT", "LON")

# Filter crime data for the year 2015 and where victim descent is known
crime_df = crime_df.withColumn("DATE OCC", to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))
crime_df = crime_df.filter((year(col("DATE OCC")) == 2015) & (col("Vict Descent").isNotNull()))

#Clean geocode df - remove null and if there is more than one zip code keep the first
geocode_df= geocode_df.filter(col("ZIPcode").isNotNull() & (col("ZIPcode") != ""))
geocode_df = geocode_df.withColumn("ZIPcode", trim(split(col("ZIPcode"), "[:;-]")[0])) 

# Join crime data with reverse geocoding data on latitude and longitude
crime_geo_df = crime_df.join(geocode_df.hint("MERGE"), (crime_df["LAT"] == geocode_df["LAT"]) & (crime_df["LON"] == geocode_df["LON"]), "inner").select(crime_df["Vict Descent"], geocode_df["ZIPcode"])

#Find LA city zip codes
la_zips = income_df.join(crime_geo_df.hint("MERGE"), crime_geo_df["ZIPcode"] == income_df["Zip Code"], "left_semi")

#Format Income column
la_zips =  la_zips.withColumn("Estimated Median Income",\
		regexp_replace(col("Estimated Median Income"), "[$,]", "").cast(IntegerType()))

#Calulate 3 zip codes with highest and 3 zip codes with lowest median income
top3 = la_zips.orderBy(desc("Estimated Median Income")).limit(3)
bottom3 = la_zips.orderBy("Estimated Median Income").limit(3)

#Find crimes that happened in top3 and bottom 3 zip codes
crimes_top3 = crime_geo_df.join(top3.hint("MERGE"), crime_geo_df['ZIPcode'] == top3['Zip Code'], 'left_semi')
crimes_bottom3 = crime_geo_df.join(bottom3.hint("MERGE"), crime_geo_df['ZIPcode'] == bottom3['Zip Code'], 'left_semi')

# Group by "Vict Descent" and count victims for each case
crimes_top3_grouped = crimes_top3.groupBy("Vict Descent").count().withColumnRenamed("count", "Total Victims")
crimes_bottom3_grouped = crimes_bottom3.groupBy("Vict Descent").count().withColumnRenamed("count", "Total Victims")

# Mapping of "Vict Descent" codes to descriptions
descent_code_map = {
    "A": "Other Asian",
    "B": "Black",
    "C": "Chinese",
    "D": "Cambodian",
    "F": "Filipino",
    "G": "Guamanian",
    "H": "Hispanic/Latin/Mexican",
    "I": "American Indian/Alaskan Native",
    "J": "Japanese",
    "K": "Korean",
    "L": "Laotian",
    "O": "Other",
    "P": "Pacific Islander",
    "S": "Samoan",
    "U": "Hawaiian",
    "V": "Vietnamese",
    "W": "White",
    "X": "Unknown",
    "Z": "Asian Indian"
}

# Convert the mapping to a DataFrame for joining
descent_code_df = spark.createDataFrame(descent_code_map.items(), ["Vict Descent", "Victim Descent"])

# Join the grouped results with the descent code descriptions
crimes_top3 = crimes_top3_grouped.join(descent_code_df.hint("MERGE"), on="Vict Descent", how="left")
crimes_bottom3 = crimes_bottom3_grouped.join(descent_code_df.hint("MERGE"), on="Vict Descent", how= "left")

#Sort with descending order of victims and final format
crimes_top3 = crimes_top3.select("Victim Descent", "Total Victims").orderBy(desc("Total Victims"))
crimes_bottom3 = crimes_bottom3.select("Victim Descent", "Total Victims").orderBy(desc("Total Victims"))

# Select and show only "Descent Description" and "count" columns
print("Crime victims grouped by Descent in ZIP Codes with Top 3 Income")
crimes_top3.show()

print("Crime victims grouped by Descent in ZIP Codes with Bottom 3 Income")
crimes_bottom3.show()

# Calculate and print elapsed time
end = time.time()
elapsed_time = end - start
print(f"Elapsed time: {elapsed_time} seconds")

# Stop the Spark session
spark.stop()
