from pyspark.sql import SparkSession
m pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("CSV to Parquet") \
    .getOrCreate() \

#csv file paths
file1 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2020_to_Present.csv"
file2 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2010_to_2019.csv"

#output paths
output1 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2020_to_Present_parquet"
output2 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2010_to_2019_parquet"

files = {file1: output1, file2: output2}

for file, output in files.items():
        df = spark.read.csv(file, header=True, inferSchema=True)
        df.write.parquet(output)



