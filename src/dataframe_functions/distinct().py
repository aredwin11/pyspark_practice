from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('shell').getOrCreate()
info=[(1,"Edwin",'M',20000),(2,"Adarsh",'M',15000),(3,"kumar",'M',40000),(2,"Adarsh",'M',15000)]
sch=['id','Name','Salary','Gender']
df=spark.createDataFrame(info,sch)
df.show()

df1=df.distinct().show()
df2=df.dropDuplicates(['Gender']).show()