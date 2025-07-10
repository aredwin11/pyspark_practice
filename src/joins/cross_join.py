from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('shell').getOrCreate()
info=[("Edwin",),("Adarsh",)]
sch=['Name']

info2=[("HR",),("IT",)]
sch2=["Department"]

df=spark.createDataFrame(info,sch)
df.show()

df1=spark.createDataFrame(info2,sch2)
df1.show()

df2=df.crossJoin(df1)
df2.show()