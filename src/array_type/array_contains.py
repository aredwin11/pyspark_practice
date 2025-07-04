from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("shell").getOrCreate()
print(spark)

info=[(1,"Edwin",["java","sql"]),(2,"kunal",["c++","dsa"])]
sch=['id','Name','skill']
df=spark.createDataFrame(info,sch)
df.show()

df2=df.withColumn("If_exists",array_contains(col('skill'),'java'))
df2.show()