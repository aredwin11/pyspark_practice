from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("shell").getOrCreate()
print(spark)

info=[(1,'Edwin','M',20000),(2,'Monica','F',40000),(3,'xyz','',50000)]
sch=["id","Name","gender","salary"]
df=spark.createDataFrame(info,sch)
df.show()

df1=df.select(df.id,df.Name,when(df.gender=='M','Male').when(df.gender=='F','Female').otherwise('Unknown').alias("Updated_gender"))
df1.show()