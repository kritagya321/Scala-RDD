// Databricks notebook source
//Importing library 
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

// COMMAND ----------

//Reading the file and filtering
val txtfile = sc.textFile("/FileStore/tables/largesttt56613_0-1.txt")
val dataDF = txtfile.filter(x=>x.length>0).zipWithIndex.toDF("text","_id").select("_id","text") 
dataDF.take(10)

// COMMAND ----------

//Using Library
val pipeline = PretrainedPipeline("recognize_entities_dl", "en")
val predictions = pipeline.transform(dataDF)
pipeline.transform(dataDF).select("entities.result")
val output = pipeline.transform(dataDF).select(explode($"entities.result"))
output.show()

// COMMAND ----------

//converting the result to rdd[row]
val finalrdd: RDD[Row] = output.rdd

// COMMAND ----------

//changing RDD[row] to RDD[String]
val sourceRdd = finalrdd.map(_.mkString(",")) 

// COMMAND ----------

//making a key-value pair with key as word and value as 1
val mappingrdd = sourceRdd.map(x => (x,1)) 

// COMMAND ----------

mappingrdd.take(10)

// COMMAND ----------

val resultRdd = mappingrdd.reduceByKey((x,y) => x+y) //Adding the count by key.

// COMMAND ----------

resultRdd.take(50)

// COMMAND ----------

//Arranging in descending number of count.
val descendingorder = resultRdd.sortBy(-_._2)
descendingorder.take(100)
