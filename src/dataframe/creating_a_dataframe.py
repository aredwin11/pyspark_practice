from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import avg,count

spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()
print(spark)
# help(spark.createDataFrame)

#Creating a dataframe
info=[(1,'Edwin'),(2,'Arun'),(3,'kamal')]
col_name=['id','Name']
ed=spark.createDataFrame(data=info,schema=col_name)
ed.show()
ed.printSchema()

#using StructType() and StructField()
info=[(1,'Edwin',23),(2,'Arun',43),(3,'kamal',36),(4,'Komal',23),(5,'tarun',23)]
struc1=StructType([StructField(name='id',dataType=IntegerType()),
                   StructField(name='name',dataType=StringType()),
                   StructField(name='age',dataType=IntegerType())])
df=spark.createDataFrame(data=info,schema=struc1)
df.show()
df.printSchema()