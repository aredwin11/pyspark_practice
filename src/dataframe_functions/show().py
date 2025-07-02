from pyspark.sql import SparkSession
# from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Show").getOrCreate()

data = [("John", "Sales Solution Architect", 1000),
        ("Mike", "Information Technology", 2000),
        ("Sara", "Information Technology", 2500),
        ("Jake", "Information science and Engineering", 1800),
        ("Lilly", "Mechanical Engineering", 2100)]

columns = ["Name", "Department", "Salary"]

df = spark.createDataFrame(data, columns)
df.show()

#Shows only the top 2 values of the table
df.show(n=2)

#Truncates the string longer than 20 char
df.show(truncate=True)

#Truncates long string to the specified length
df.show(truncate=10)

#Returns the entire string without truncating
df.show(truncate=False)

#return the output line by line for each value
df.show(vertical=True)
