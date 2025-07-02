from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("withColumn").getOrCreate()
print(spark)

info=[(1,'Edwin',20000),(2,'Karan',12000),(3,'arun',30000)]
sch=['id','name','salary']

df=spark.createDataFrame(info,sch)
df.show()
df.printSchema()

df2=df.withColumnRenamed('salary','salary_amount')
df2.show()