from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
import numpy as np

# Spark session setup
spark = SparkSession.builder \
 .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.15.1-beta') \
 .getOrCreate()

# BigQuery dataset selection, tables definition
cell = "d"
machine_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.machine_events"
collection_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.collection_events"
instance_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.instance_events"
usage_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.instance_usage"
attrs_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.machine_attributes"

# Count usage table
tbl = usage_tbl
df = spark.read.format("bigquery").option("table", tbl).load()
df.count()

tbl = usage_tbl
source1 = spark.read.format("bigquery").option("table", tbl).load().select("start_time", "end_time", "average_usage.cpus")
source2 = spark.createDataFrame(source1.take(1000000)).repartition(300) # limited for testing

# Minute-per-minute average CPU load
df = source1
df = df.withColumn("duration", (df.end_time - df.start_time) / 1e6);
df = df.filter(df.duration < 60);
df = df.withColumn("cpu_seconds", df.cpus * df.duration);
df = df.withColumn("minute_index", sf.floor(df.start_time / 60e6));

result_df = df.groupBy("minute_index").agg(sf.sum(df.cpu_seconds));
result_df.show()

arr = np.array(result_df.collect())
arr.dump("./cpu_seconds.npy")