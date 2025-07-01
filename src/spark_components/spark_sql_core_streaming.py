#creating a spark session
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark=SparkSession.builder.appName("Databrick shell")\
                    .master("local[8]")\
                    .getOrCreate()
print(spark)

# Spark Core - Creating a RDD
rdd=spark.sparkContext.parallelize(["apple","core"])
result=rdd.collect()
print(result)

#Spark SQL
data=[(1,"Edwin",22),(2,"Rahul",19),(3,"kumar",32)]
df=spark.createDataFrame(data,["Id","Name","Age"])
df.show()

df.createOrReplaceTempView("Employee")
res=spark.sql("SELECT * FROM Employee WHERE Age>25")
res.show()

#SQL streaming
ssc=StreamingContext(spark.sparkContext,batchDuration=5)
dstrem=ssc.socketTextStream("localhost",9999)
dstrem.pprint()
ssc.start()
ssc.awaitTermination()
