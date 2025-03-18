from pyspark.sql import SparkSession
spark = SparkSession.builder \
 .appName('BigQuery Storage & Spark DataFrames') \
 .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.15.1-beta') \
 .getOrCreate()

cell = "d"

machine_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.machine_events"
collection_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.collection_events"
instance_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.instance_events"
usage_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.instance_usage"
attrs_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.machine_attributes"

from pyspark.sql.functions import count_distinct, avg , col , from_unixtime, window

tbl = usage_tbl
df = spark.read.format("bigquery").option("table", tbl).load()
df = df.withColumn("timestamp_seconds", (col("timestamp") / 1000).cast("timestamp"))
result_df = df.groupBy(window(col("timestamp_seconds"), "1 minute")).agg(avg("average_usage.cpu").alias("avg_cpu_usage"))
result_df.select("window.start", "window.end", "avg_cpu_usage").show()

from pyspark.sql.functions import count_distinct, avg , col , from_unixtime, window

tbl = usage_tbl
df = spark.read.format("bigquery").option("table", tbl).load()
df.limit(1).show()

from pyspark.sql.functions import count_distinct, avg , col , from_unixtime, window

tbl = usage_tbl
df = spark.read.format("bigquery").option("table", tbl).load()
df = df.withColumn("timestamp_seconds", (col("start_time") / 1000).cast("timestamp"))
result_df = df.groupBy(window(col("timestamp_seconds"), "1 minute")).agg(avg("average_usage.cpus").alias("avg_cpu_usage"))
result_df.select("window.start", "window.end", "avg_cpu_usage").show()

from pyspark.sql.functions import count_distinct, avg , col , from_unixtime, window

tbl = usage_tbl
df = spark.read.format("bigquery").option("table", tbl).load()

df.select("start_time", "end_time", "average_usage").show()