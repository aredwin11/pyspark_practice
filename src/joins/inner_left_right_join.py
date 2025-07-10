from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('shell').getOrCreate()
info=[(1,"Edwin",'2',20000),(2,"Adarsh",'1',15000),(3,"kumar",'3',40000),(4,"Barath",4,23000)]
sch=['id','Name','dep_id','Salary']

info2=[(1,"HR"),(2,"IT"),(5,"Construction")]
sch2=['id',"Department"]

df=spark.createDataFrame(info,sch)
df.show()

df1=spark.createDataFrame(info2,sch2)
df1.show()

#Inner join() --> Returns the common values from both table
df.join(df1,df.dep_id==df1.id,'inner').show()

#Left Join() --> Returns all the values from left table and common values from the right table
df.join(df1,df.dep_id==df1.id,'left').show()

#Right join() --> Returns all the values from right table and common values from the left table
df.join(df1,df.dep_id==df1.id,'right').show()



