from pyspark.sql import SparkSession

spark = SparkSession.builder\
            .appName("test connection rest iceberg gravitino")\
                .master('local[*]')\
                .getOrCreate()

print("Création du namespace: default")
spark.sql("CREATE NAMESPACE IF NOT EXISTS default;").show(truncate=False)

print("Listes des namespaces:")                
spark.sql("show namespaces;").show(truncate=False)



spark.stop()