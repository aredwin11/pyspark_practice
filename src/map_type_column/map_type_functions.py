from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName("shell3").getOrCreate()
info=[("Edwin",{1:"Java",2:"sql"}),("Akash",{1:"python",2:"Django"})]

sch=StructType([StructField("name",StringType()),
                 StructField("skills",MapType(IntegerType(),StringType()))])
df=spark.createDataFrame(info,sch)
df.show(truncate=False)

#explode()--> splits the key and value and returns them new column
df1=df.select("name","skills",explode(df.skills))
df1.show(truncate=False)

df2=df.withColumn("key",map_keys(df.skills))
df2.show(truncate=False)

df3=df.withColumn("value",map_values(df.skills))
df3.show(truncate=False)

