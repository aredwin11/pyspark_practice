from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("shell").getOrCreate()
print(spark)

info=[(1,"Edwin","java","sql"),(2,"kunal","c++","dsa")]
sch=['id','Name','Primary_skill','Secondary_skill']
df=spark.createDataFrame(info,sch)
df.show()

df1=df.withColumn("combined_skill",array(col('Primary_skill'),col('Secondary_skill')))
df1.show()

