from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("databricks cell")\
                            .master("local[8]")\
                            .getOrCreate()
print(spark)

df = spark.read.parquet("/Volumes/my_catalog/my_schema/my_volume/sampleuserdata.parquet")
df.show()
df.printSchema()
print("Total number of rows are :" ,df.count())
