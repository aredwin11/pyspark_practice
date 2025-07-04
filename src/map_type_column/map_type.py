from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName("Shell2").getOrCreate()
print(spark)

info=[("Edwin",{1:"java",2:"python"}),("abhi",{1:"DSA",2:"sql"})]
sch=StructType([StructField('name',StringType()),\
                StructField('skill',MapType(IntegerType(),StringType()))])
df=spark.createDataFrame(info,sch)
df.show(truncate=False)
df.printSchema()

df1=df.withColumn('primary_skill',df.skill[1])
df1.show()

df2=df1.withColumn('Secondary_skill',df.skill.getItem(2))
df2.show()

