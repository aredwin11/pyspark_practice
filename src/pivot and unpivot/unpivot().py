from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("DepartmentGenderCount").getOrCreate()


data = [
    ('IT', 8, 5),
    ('Payroll', 3, 2),
    ('HR', 2, 4)
]
schema = ['dep', 'male', 'female']
df = spark.createDataFrame(data, schema)
df.show()

df.select('dep',expr("stack(2,'male',male,'female',female)as(gender,count)")).show()
