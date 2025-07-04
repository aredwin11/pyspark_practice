from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName("shell").getOrCreate()
print(spark)

info=[(1,"Edwin",("Black","Blue")),(2,"Arya",("Brown","Gray"))]
propsType=StructType([StructField('Hair_Colour',StringType()),\
                      StructField('Eye_Colour',StringType())])

Sch=StructType([StructField('id',IntegerType()),
                StructField('Name',StringType()),
                StructField('Properties',propsType)])

df=spark.createDataFrame(info,Sch)
df.show()

df1=df.select(df.Properties).show()