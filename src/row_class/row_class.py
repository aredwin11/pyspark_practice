from pyspark.sql import SparkSession
from pyspark.sql import Row

spark=SparkSession.builder.appName("shell").getOrCreate()
print(spark)

row=Row("Edwin",20000)
print(row[0]+' - '+str(row[1]))

row=Row(name="arun",salary=20001)
print(row.name+' - '+str(row.salary))

person=Row('Name','Salary')
p1=person('Edwin',20000)
p2=person('Arun',20020)
# print(p1.Name+' - '+str(p1.Salary))
data=[p1,p2]
df=spark.createDataFrame(data)
df.show()

