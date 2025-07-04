from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("shell").getOrCreate()
print(spark)

col1=lit('welcome')
print(type(col1))

info=[(1,'Edwin'),(2,'Arun'),(3,'kamal')]
col_name=['id','Name']
ed=spark.createDataFrame(data=info,schema=col_name)
ed.show()

#Accessing name column in multiple ways
ed.select(ed.Name).show()

ed.select(ed['Name']).show()

ed.select(col('Name')).show()

