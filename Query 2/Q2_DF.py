from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lower
from pyspark.sql.functions import floor
import time

# Start time measurement
start = time.time()

# Δημιουργία SparkSession
spark = SparkSession\
	.builder \
	.appName("Q2 DataFrame") \
	.getOrCreate()


# Διαδρομές προς τα αρχεία CSV
csv_file_path1 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2010_to_2019.csv"
csv_file_path2 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2020_to_Present.csv"

# Φόρτωση των datasets
df1 = spark.read.option("header", "true").csv(csv_file_path1)
df2 = spark.read.option("header", "true").csv(csv_file_path2)

# Συνένωση των δύο DataFrames
df1 = df1.select("TIME OCC", "Premis Desc")
df2 = df2.select("TIME OCC", "Premis Desc")
df = df1.union(df2)

# Φιλτράρισμα για τα εγκλήματα που έλαβαν χώρα στο δρόμο (STREET)
df = df.filter(col("Premis Desc") == "STREET")

# Μετατροπή της στήλης TIME OCC σε ώρα
df = df.withColumn("Hour", col("TIME OCC").cast("int") / 100)
df = df.withColumn("Hour", floor(col("Hour")).cast("int"))


# Καθορισμός τμήματος της ημέρας βάσει της ώρας
df = df.withColumn("Day_Part", when((col("Hour") >= 5) & (col("Hour") < 12), "Πρωί")
                              .when((col("Hour") >= 12) & (col("Hour") < 17), "Απόγευμα")
                              .when((col("Hour") >= 17) & (col("Hour") < 21), "Βράδυ")
                              .otherwise("Νύχτα"))


# Ομαδοποίηση και καταμέτρηση των εγκλημάτων ανά τμήμα της ημέρας
crime_counts = df.groupBy("Day_Part").agg(count("*").alias("Crime_Count"))

# Ταξινόμηση των αποτελεσμάτων σε φθίνουσα σειρά
sorted_crime_counts = crime_counts.orderBy(col("Crime_Count").desc())

# Εμφάνιση των αποτελεσμάτων
sorted_crime_counts.show()

# Calculate and print elapsed time
end = time.time()
elapsed_time = end - start
print(f"Elapsed time: {elapsed_time} seconds")

# Κλείσιμο του SparkSession
spark.stop()
