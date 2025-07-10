from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GenderDepartmentDF").getOrCreate()
data = [
    (1, 'maheer', 'male', 'IT'),
    (2, 'wafa', 'male', 'IT'),
    (3, 'asi', 'female', 'HR'),
    (4, 'annu', 'female', 'IT'),
    (5, 'shakti', 'female', 'IT'),
    (6, 'pradeep', 'male', 'HR'),
    (7, 'sarfaraj', 'male', 'HR'),
    (8, 'ayesha', 'female', 'IT')
]
schema = ['id', 'name', 'gender', 'dep']
df = spark.createDataFrame(data, schema)
df.show()

df.groupby('dep','gender').count().show()

df.groupby('dep').pivot('gender').count().show()

df.groupby('dep').pivot('gender',['male']).count().show()