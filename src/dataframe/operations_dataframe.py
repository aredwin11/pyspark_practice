from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import avg,count

spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()
print(spark)

info=[(1,'Edwin',23),(2,'Arun',43),(3,'kamal',36),(4,'Komal',23),(5,'tarun',23)]
struc1=StructType([StructField(name='id',dataType=IntegerType()),
                   StructField(name='name',dataType=StringType()),
                   StructField(name='age',dataType=IntegerType())])
df=spark.createDataFrame(data=info,schema=struc1)
df.show()
df.printSchema()

# operations on dataframes
# 1. Filter()
df.filter(df.age>30).show()

df.where(df.age<30).show()
#2. select()
df.select('name').show()

#3.Aggregate functions
df.agg(avg('age')).show()
df.agg(count(df.id)).show()
df.groupby('age').count().show()