from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("databricks cell")\
                            .master("local[8]")\
                            .getOrCreate()
print(spark)


#Reading a single json file using json()
df=spark.read.json("practice.json",multiLine=True)
df.show()

#reading a single json file using load()
sch=StructType([StructField('id',IntegerType()),
                StructField('age',IntegerType()),
                StructField('name',StringType()),
                StructField('isActive',StringType()),
                StructField('department',StringType())])
df=spark.read.format("json").schema(sch).option("multiline", True).load("practice.json")
df.show()

# Reading multiple json file into dataframe using json()
df=spark.read.json(path=['practice.json','practice2.json'],multiLine=True)
df.show()


#Reading multiple json file into dataframe using load()
df=spark.read.format("json").option("multiline", True).load(["practice.json","practice2.json"])
df.show()