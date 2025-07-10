from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Shell").getOrCreate()
print(spark)

info=[(1,"Edwin",2000),(2,"Ajay",4000),(3,"Krishna",3000)]
sch=['id','Name','Salary']
df=spark.createDataFrame(info,sch)
df.show()

df.sort(df.Salary.asc()).show()
df.sort(df.Salary.desc()).show()
df.sort(df.Name).show()