from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()
data = [
    (1, 'Maheer', 'male', 1000, None),
    (2, 'Asi', 'Female', 2000, 'IT'),
    (3, 'abcd', None, 1000, 'HR')
]
schema = ['id', 'name', 'gender', 'salary', 'dep']


df = spark.createDataFrame(data, schema)
df.show()

df.na.fill('others').show()

df.fillna('others',['gender']).show()

df.na.fill('others').show()

