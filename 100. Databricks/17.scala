// Lab - Streaming data into the table

import org.apache.spark.sql.types._
import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions._

val connectionString = "Endpoint=sb://appnamespace400010.servicebus.windows.net/;SharedAccessKeyName=PolicyA;SharedAccessKey=uV3YAzIE+hR9OqKd9qX+6mcCuYInMXWwChBaDO8Lde8=;EntityPath=dbmultihub"
val eventHubsConf = EventHubsConf(connectionString)
.setStartingPosition(EventPosition.fromStartOfStream)


val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)  
  .load()
  .select(get_json_object(($"body").cast("string"), "$.records").alias("records"))


val maxMetrics = 100 
val jsonElements = (0 until maxMetrics)
                     .map(i => get_json_object($"records", s"$$[$i]"))


val newDF = eventhubs
  .withColumn("records", explode(array(jsonElements: _*))) 
  .where(!isnull($"records")) 

val dataSchema = new StructType()
        .add("count", LongType)
        .add("total", DoubleType)
        .add("minimum", DoubleType)
        .add("maximum", DoubleType)
        .add("resourceId", StringType)
        .add("time", DataTypes.DateType)
        .add("metricName", StringType)
        .add("timeGrain", StringType)
        .add("average", DoubleType)

val df=newDF.withColumn("records",from_json(col("records"),dataSchema))


val finalDF=df.select(col("records.*"))

// Writing to the delta lake table
finalDF.writeStream
.format("delta")
.outputMode("append")
.option("checkpointLocation", "/delta/events/_checkpoints/metrics")
.table("newmetrics")


// In another data frame

import org.apache.spark.sql.functions._

display(spark.readStream
.format("delta")
.table("newmetrics")
.groupBy($"metricName",window($"time","10 seconds")).count().orderBy("window"))
