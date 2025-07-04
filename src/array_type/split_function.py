from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("shell").getOrCreate()
print(spark)

info=[(1,"Edwin","java,sql"),(2,"kunal","c++,dsa")]
sch=['id','Name','skills']
df=spark.createDataFrame(info,sch)
df.show()

#split() -->This sql function returns an arrayType after splitting the string column by delimiter
df1=df.withColumn("Skills_Array",split(col('skills'),','))
df1.show()