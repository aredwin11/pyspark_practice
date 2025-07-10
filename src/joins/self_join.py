from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("shell").getOrCreate()
print(spark)

data = [
    (1, "Alice", None),
    (2, "Bob", 1),
    (3, "Charlie", 1),
    (4, "David", 2),
    (5, "Eve", 2)
]

columns = ["emp_id", "name", "mgr_id"]
df = spark.createDataFrame(data, columns)
df.show()


df.alias('emp_name').join(df.alias('mag_name'),
    col("emp_name.mgr_id")==col("mag_name.emp_id"),'inner').select(
    col('emp_name.name').alias("employee"),
    col('mag_name.name').alias("Manager"),
).show()


