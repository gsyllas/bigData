from pyspark import SparkContext, SparkConf
import csv
from io import StringIO
import time

start = time.time()

# Δημιουργία SparkContext
conf = SparkConf().setAppName("Q2 RDD")
sc = SparkContext.getOrCreate(conf=conf)

# Διαδρομές προς τα αρχεία CSV
csv_file_path1 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2010_to_2019.csv"
csv_file_path2 = "hdfs://master:9000/home/user/ergasia_data/Crime_Data_from_2020_to_Present.csv"

# Συνάρτηση για να διαβάσει μια γραμμή CSV σωστά
# Eπιστρέφει μόνο τις στήλες "TIME OCC", "Premis Desc"  
def parse_csv(line):
    sio = StringIO(line)
    reader = csv.reader(sio)
    row = next(reader)
    return row[3], row[15]

# Φόρτωση των datasets ως RDDs
rdd1 = sc.textFile(csv_file_path1).map(parse_csv)
rdd2 = sc.textFile(csv_file_path2).map(parse_csv)

# Σύνδεση των RDDs
rdd = rdd1.union(rdd2)

# Λήψη του header και φιλτράρισμα του από τα δεδομένα
header = rdd.first()
rdd = rdd.filter(lambda row: row != header)

# Φιλτράρισμα για τα εγκλήματα που έλαβαν χώρα στο δρόμο 
rdd_street = rdd.filter(lambda row: row[1] == "STREET")

# Συνάρτηση για τον καθορισμό του τμήματος της ημέρας βάσει της ώρας
def get_day_part(time_occ):
    hour = int(time_occ) // 100
    if 5 <= hour < 12:
        return "Πρωί"
    elif 12 <= hour < 17:
        return "Απόγευμα"
    elif 17 <= hour < 21:
        return "Βράδυ"
    else:
        return "Νύχτα"

#Εξαγωγή του τμήματος της ημέρας και επιστροφή (day_part,1) για κάθε γραμμή) 
rdd_day_part = rdd_street.map(lambda row: (get_day_part(row[0]), 1))

#Reduce by key για καταμέτρηση εγκλημάτων κάθε day_part και ταξινόμηση κατά φθίνουσα τιμή
day_part_counts = rdd_day_part.reduceByKey(lambda a, b: a + b) \
                            .sortBy(lambda x: x[1], ascending=False)

# Συλλογή και εμφάνιση των αποτελεσμάτων
results = day_part_counts.collect()
for part, count in results:
    print(f"{part}: {count}")

#calculate and print elapsed time
end = time.time()
elapsed_time = end - start
print(f"Elapsed time: {elapsed_time} seconds")

# Κλείσιμο του SparkContext
sc.stop()
