from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Databrick shell")\
                    .master("local[8]")\
                    .getOrCreate()
print(spark)