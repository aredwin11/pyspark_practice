from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("ArrayType_functions").getOrCreate()
print(spark)

info=[(1,"Edwin",["spark","databricks","pyspark"]),(1,"Kiran",["java","HTML","dotnet"])]
sch=["id","Name","Skills"]
df=spark.createDataFrame(info,sch)
df.show(truncate=False)
df.printSchema()

#Explode() --> returns new row for each element
df1=df.withColumn("Skill",explode(col("skills")))
df1.show()
