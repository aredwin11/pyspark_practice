from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Shell").getOrCreate()
print(spark)

info=[(1,"Edwin",2000),(2,"Ananya",4000),(3,"Krishna",3000)]
sch=['id','Name','Salary']
df=spark.createDataFrame(info,sch)
df.show()

#like() --> returns the name that starts with E
df.filter(df.Name.like('E%')).show()

#like() --> returns the name that ends with a
df.filter(df.Name.like('%a')).show()