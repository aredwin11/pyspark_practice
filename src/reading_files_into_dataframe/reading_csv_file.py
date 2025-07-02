# Creating a spark session
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("databrick cell")\
                            .master("local[8]")\
                            .getOrCreate()
print(spark)

# help() to see the documentation on spark.read
help(spark.read)

#Reading a single csv file using csv()
schema=StructType().add('id',StringType())
df=spark.read.csv(path='Department-Q1.csv',header=True,schema=schema)
df.show()

#Reading a single csv file using load()
df=spark.read.format('csv').option(key='header',value='True').load(path='Employee-Q1.csv')
df.show()
df.printSchema()

#Reading a multiple csv file using csv()
df=spark.read.csv(path=['Department-Q1.csv','Employee-Q1.csv'],header=True)
df.show()
df.printSchema()

#Reading a multiple csv file using load()
df=spark.read.format('csv').option(key='header',value='True').load(path=['Employee-Q1.csv','Department-Q1.csv'])
df.show()