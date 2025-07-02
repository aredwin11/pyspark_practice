from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("ArrayType").getOrCreate()
print(spark)

info=[('Edwin',[11,4]),('Akash',[21,8])]
sch=StructType([StructField('name',StringType()),
                StructField('num',ArrayType(IntegerType()))])
df=spark.createDataFrame(info,sch)
df.show()
df.printSchema()

#Accessing the element at 0 index
df1=df.withColumn('FirstNumbers',col=col('num')[0])
df1.show()