from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("CSV to Parquet") \
    .getOrCreate() \

#csv file paths
file1 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2020_to_Present.csv"
file2 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2010_to_2019.csv"

#output path
output = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_parquet"

df1 = spark.read.csv(file1, header=True, inferSchema=True)
df2 = spark.read.csv(file2, header=True, inferSchema=True)

combined_df = df1.union(df2)

combined_df.write.parquet(output)

spark.stop()


