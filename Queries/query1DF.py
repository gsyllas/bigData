from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, desc
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import time
spark = SparkSession.builder \
    .appName("CrimeStatistics") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Διαδρομές προς τα αρχεία CSV
csv_file_path_1 = "C:/Users/gsyllas/Dsml/BigData/Crime_Data_from_2010_to_2019.csv"
csv_file_path_2 = "C:/Users/gsyllas/Dsml/BigData/Crime_Data_from_2020_to_Present.csv"
start = time.time()

# Φόρτωση των datasets
df1 = spark.read.option("header", "true").csv(csv_file_path_1)
df2 = spark.read.option("header", "true").csv(csv_file_path_2)

# Μετατροπή των στηλών ημερομηνίας σε κατάλληλη μορφή
df1 = df1.withColumn("Date_Rptd", F.to_date(col("Date Rptd"), "M/d/yyyy H:mm"))
df2 = df2.withColumn("Date_Rptd", F.to_date(col("Date Rptd"), "M/d/yyyy H:mm"))

# Προσθήκη των στηλών "Year" και "Month"
df1 = df1.withColumn("Year", year(col("Date_Rptd"))).withColumn("Month", month(col("Date_Rptd")))
df2 = df2.withColumn("Year", year(col("Date_Rptd"))).withColumn("Month", month(col("Date_Rptd")))

# Συνένωση των δύο dataframes
df = df1.union(df2)

# Ομαδοποίηση και καταμέτρηση των εγκλημάτων ανά μήνα και έτος
monthly_crime_counts = df.groupBy("Year", "Month").count()

# Ορισμός παραθύρου για την κατάταξη των μηνών ανά αριθμό εγκλημάτων
window = Window.partitionBy("Year").orderBy(desc("count"))

# Προσθήκη της κατάταξης σε κάθε γραμμή
ranked_df = monthly_crime_counts.withColumn("Rank", F.rank().over(window))

# Επιλογή των κορυφαίων 3 μηνών ανά έτος
top3_months_per_year = ranked_df.filter(col("Rank") <= 3).orderBy("Year", desc("count"))

# Εμφάνιση των αποτελεσμάτων
top3_months_per_year.select("Year", "Month", "count", "Rank").show(50)
end = time.time()
elapsed_time = end - start
print(f"Elapsed time: {elapsed_time} seconds")
# Κλείσιμο του SparkSession
spark.stop()
