// Databricks notebook source
sc.appName

// COMMAND ----------

print(spark)

// COMMAND ----------

println(sc)
println(sqlContext)

// COMMAND ----------

val myData:List[String]=List("Alice","Carlos","Frank","Barbara")
val myRdd=sc.parallelize(myData)
myRdd.take(2)

// COMMAND ----------

/*Creating RDD of three elements from a scala sequence with 2 partitions by using parallelize method*/
val x=sc.parallelize(Array(4,5,6),2)

// COMMAND ----------

/*Collect action in RDD 
Glom()-Return an RDD created by coalescing all elements withing each partition into a list*/
val x=sc.parallelize(Array(4,5,6),2)
val y=x.collect()
val xout=x.glom().collect()
println(y)

// COMMAND ----------

/*Finding number of partitions in RDD x */
x.getNumPartitions

// COMMAND ----------

/*Performing take action in RDD*/
x.take(2)

// COMMAND ----------

val x=sc.parallelize(Array("m","n","o"))//RDD x:[m,n,o]
val y=x.map(z=>(z,1))//map x into RDD y:[(m,1),(n,1),(o,1)]

// COMMAND ----------

println(x.collect().mkString(","))
println(y.collect().mkString(","))

// COMMAND ----------

val x=sc.parallelize(Array(3,4,5))
val y=x.filter(n=>n%2==1)

// COMMAND ----------

println(x.collect().mkString(","))
println(y.collect().mkString(","))

// COMMAND ----------

val x=sc.parallelize(Array(1,2,3,4))
val y=x.reduce((a,b)=>a+b)

// COMMAND ----------

println(x.collect.mkString(","))
println(y)

// COMMAND ----------

val x=sc.parallelize(Array(1,2,3))
val y=x.flatMap(n=>Array(n,n*100,50))

// COMMAND ----------

println(x.collect().mkString(","))
println(y.collect().mkString(","))

// COMMAND ----------

val words = sc.parallelize(Array("a", "b", "a", "a", "b", "b", "a", "a", "a", "b", "b"))
words.collect()

// COMMAND ----------

val wordCountPairRDD = words.map(s=>(s,1))
wordCountPairRDD.collect()

// COMMAND ----------

val wordcounts = wordCountPairRDD.reduceByKey((value1, value2)=> value1+value2)
wordcounts.collect()

// COMMAND ----------

val words = sc.parallelize(Array("a","b","a","a","b","b","a","a","a","b","b"))
val wordcounts = words.map(s => (s,1)).reduceByKey(_+_).collect()

// COMMAND ----------

/*Sort by key*/
val words = sc.parallelize(Array("a","b","a","a","b","b","a","a","a","b","b"))
val wordCountPairRDD = words.map(s => (s,1))
val wordCountPairRDDSortedByKey = wordCountPairRDD.sortByKey()

// COMMAND ----------

wordCountPairRDD.collect()

// COMMAND ----------

wordCountPairRDDSortedByKey.collect()

// COMMAND ----------

val wordCountPairRDDGroupByKey = wordCountPairRDD.groupByKey().collect()

// COMMAND ----------

val list = 1 to 10
var sum = 0
list.map(x => sum = sum + x)
print(sum)

// COMMAND ----------

val rdd = sc.parallelize(1 to 10)
var sum = 0

// COMMAND ----------

val rdd1 = rdd.map(x => sum = sum +x).collect()

// COMMAND ----------

val rdd1 = rdd.map(x =>
                  {var sum = 0;
                        sum = sum + x
                        sum}
                  ).collect()

// COMMAND ----------


