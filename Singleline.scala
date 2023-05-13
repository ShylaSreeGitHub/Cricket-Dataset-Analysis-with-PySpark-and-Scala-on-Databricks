// Databricks notebook source
//Reading json file
val df=sqlContext.read.json("dbfs:/FileStore/shared_uploads/shylasreekonda@my.unt.edu/Singleline.json")

// COMMAND ----------

//Printing the schema
df.printSchema()

// COMMAND ----------

//saving as temporary table
df.createTempView("Json")

// COMMAND ----------

val data = sqlContext.sql("select * from Json")

// COMMAND ----------

data.show()

// COMMAND ----------


