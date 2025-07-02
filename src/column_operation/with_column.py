from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("withColumn").getOrCreate()
print(spark)

info=[(1,'Edwin',20000),(2,'Karan',12000),(3,'arun',30000)]
sch=['id','name','salary']

df=spark.createDataFrame(info,sch)
df.show()
df.printSchema()

#to view the documentation of withColumn
help(df.withColumn)

df1=df.withColumn('Bonus_salary',df.salary+200)
df1.show()

df1=df.withColumn('salary',df.salary+200)
df1.show()

df3=df.withColumn('last_name',lit('kumar'))
df3.show()

df2=df.withColumn('salary',col=col('salary').cast('Integer'))
df2.show()
df2.printSchema()


df4=df.withColumns({
    'name':lit('arun'),
    'salary':col('salary')+20
})
df4.show()