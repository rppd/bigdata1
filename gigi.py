from pyspark.sql import SparkSession
spark = SparkSession.builder \
 .appName('BigQuery Storage & Spark DataFrames') \
 .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.15.1-beta') \
 .getOrCreate()

cell = "d"
#les différentes tables du dataset
machine_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.machine_events"
collection_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.collection_events"
instance_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.instance_events"
usage_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.instance_usage"
attrs_tbl = f"google.com:google-cluster-data.clusterdata_2019_{cell}.machine_attributes"


# ------------------------ 1 ------------------------------------------------------
# dans la table des attributs, calculer le nombre moyen d'attributs par machines
# on voit une nette différence en fonction de sur combien de ligne on lance le calculs, en se limitant aux 1000 premières on a un résultat aux alentours de 2 attributs alors que sur le total on dépasse la centaine
# les attributs peuvent être très divers, comme par exemple avoir ou non une adresse IPv4 attribuée
from pyspark.sql.functions import count_distinct, avg , col , from_unixtime, window, count

tbl = attrs_tbl
df = spark.read.format("bigquery").option("table", tbl).load()
attributes_per_machine = df.groupBy("machine_id").agg(count("name").alias("attribute_count"))
average_attributes_per_machine = attributes_per_machine.agg(avg("attribute_count").alias("avg_attributes")).collect()[0]["avg_attributes"]

print(average_attributes_per_machine)


# ------------------------ 2 ------------------------------------------------------

# Dans la table des "CollectionEvents" sont répertoriés les jobs crées ainsi que les allocations de ressources faites. 
# Etant donné que le dataset contient à la fois des "time-series of thing changes" et des "periodic snapshots of thing state", il est possible de regarder les différences et de voir si on a des données qui manquent.
# Dans le cas où il manque de la donnée, une donnée "cohérente" vis à vis de ses voisines et mise à la place et le champs missing_type est rempli (avec différentes valeurs en fonction du type d'absence
# Le but du calcul est donc de voir à quel point notre données est véridique et à quel point elle a été completées, ainsi pas besoin de regarder les différents types de manque, ici on regarde juste si elle a été frabriquée ou non:

from pyspark.sql.functions import count_distinct, avg , col , from_unixtime, window, count, when, sum

tbl = collection_tbl
df = spark.read.format("bigquery").option("table", tbl).load()

missing_stats_df = df.withColumn("is_missing_type_null",when(col("missing_type").isNull(), "NULL").otherwise("NOT NULL"))
missing_counts = missing_stats_df.groupBy("is_missing_type_null").agg(count("*").alias("count"))
total_count = missing_counts.agg(sum("count").alias("total")).collect()[0]["total"]
missing_percentages = missing_counts.withColumn("percentage",(col("count") / total_count) * 100)

missing_percentages.show()
# on a obtenu un résultat rassurant :
#is_missing_type_null|   count|          percentage|
#+--------------------+--------+--------------------+
#|            NOT NULL|     206|0.001832280187305...|
#|                NULL|11242616|    99.9981677198127|



# ------------------------3 ------------------------------------------------------

# Toujours dans la table collection, on s'intéresse ici aux vertical scaling et plus précisément à quel point celui est automatisé ou à l'inverse controlé manuellement par l'utilisateur
# On obtient simplement les différents pourcentages, pour un vertical scaling automatisé, automatisé mais avec des limites explicitées par l'utilisateu, entièrement manuel ou enfin dans de rare cas , pas de données
from pyspark.sql.functions import count_distinct, avg , col , from_unixtime, window, count, when, sum

tbl = collection_tbl
df = spark.read.format("bigquery").option("table", tbl).load()

scaling_counts = df.groupBy("vertical_scaling").agg(count("*").alias("count"))

total_count = scaling_counts.agg(sum("count").alias("total")).collect()[0]["total"]

scaling_percentages = scaling_counts.withColumn("percentage",(col("count") / total_count) * 100)

# Afficher les résultats
updated_df = scaling_percentages.withColumn(
    "vertical_scaling",
    when(col("vertical_scaling") == "NULL", "UNKNOWN")
    .when(col("vertical_scaling") == 1, "OFF")
    .when(col("vertical_scaling") == 2, "CONSTRAINED")
    .when(col("vertical_scaling") == 3, "FULLY_AUTOMATED")
    .otherwise("UNKNOWN")  
)
updated_df.show()

#Résultats 
#vertical_scaling|  count|          percentage|
#+----------------+-------+--------------------+
#|         UNKNOWN|     10|8.894564016045082E-5|
#|             OFF|1813467|  16.129998322485225|
#| FULLY_AUTOMATED|5664906|   50.38686906187788|
#|     CONSTRAINED|3764439|   33.48304366999673
