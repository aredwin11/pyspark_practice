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

#Leftsemi() -->left_semi join returns only the rows from the left DataFrame that have matching keys in the right DataFrame,
# but it does not return any columns from the right DataFrame.
df.join(df1,df.dep_id==df1.id,'leftsemi').show()

#Leftanti() -->left_semi join returns only the rows from the left DataFrame that have matching keys in the right DataFrame,
# but it does not return any columns from the right DataFrame.
df.join(df1,df.dep_id==df1.id,'leftanti').show()




