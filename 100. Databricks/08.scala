
// Lab - Structured Streaming - Let's first understand our data
// Here we are reading the information from the sample JSON-based file
import org.apache.spark.sql.functions._

val df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/techsup1000@gmail.com/PT1H.json")
display(df1.select(col("count"),col("minimum"),col("maximum"),col("resourceId"),col("time"),col("metricName")))