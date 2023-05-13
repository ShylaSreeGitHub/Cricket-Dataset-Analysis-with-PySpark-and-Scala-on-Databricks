// Databricks notebook source
//Display first 600 bytes of the file
dbutils.fs.head("dbfs:/FileStore/shared_uploads/shylasreekonda@my.unt.edu/Wordcount_Activity2.txt",600)

// COMMAND ----------

val SouAddress = sc.textFile("dbfs:/FileStore/shared_uploads/shylasreekonda@my.unt.edu/Wordcount_Activity2.txt")

// COMMAND ----------

SouAddress.count()

// COMMAND ----------

SouAddress.take(4).foreach(println)

// COMMAND ----------

SouAddress.collect

// COMMAND ----------

SouAddress.cache()

// COMMAND ----------

SouAddress.flatMap(line => line.split(" ")).take(50)

// COMMAND ----------

SouAddress
.flatMap(line => line.split(" "))
.map(word=>(word,1))
.reduceByKey(_+_)
.collect()

// COMMAND ----------

val SouAddress_Wordcount = SouAddress
.flatMap(line => line.replaceAll("\\s+", " ")
  //replace multiple whitespace characters (including space, tab, new, line, etc.) with one whitespace " "
            .replaceAll("""([,?.!:;])""","")
//replace the following punctions characters: , ? . ! : ; . with the empty string ""
            .toLowerCase()
// converting to lower-case
            .split(" "))
.map(x => (x,1))
.reduceByKey(_+_)
.collect()

// COMMAND ----------

val top10 = SouAddress_Wordcount.sortBy(_._2).reverse.take(10)

// COMMAND ----------


