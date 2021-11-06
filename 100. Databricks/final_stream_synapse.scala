// Now we need to display our data properly
import org.apache.spark.sql.types._
import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions._

spark.conf.set(
    "fs.azure.account.key.datalake2000.dfs.core.windows.net",
    dbutils.secrets.get(scope="data-lake-key",key="datalake2000"))

val connectionString = "Endpoint=sb://appnamespace400010.servicebus.windows.net/;SharedAccessKeyName=databricks;SharedAccessKey=QdXDjqMcqt0jeFlcrYx9SjmBS4orqfIcBkHpEycUnks=;EntityPath=dbhub"
val eventHubsConf = EventHubsConf(connectionString)
.setStartingPosition(EventPosition.fromStartOfStream)
// Reference for the different options - https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/streaming-event-hubs


// get_json_object - Extracts json object from a json string
// Here we want to get the body part of the messages that are sent from Azure Event Hub
// The body of the messages have to be first converted to string
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)  
  .load()
  .select(get_json_object(($"body").cast("string"), "$.records").alias("records"))

// Next we want to extract all of the json objects. Each metric will be a seperate JSON object
val maxMetrics = 30 
val jsonElements = (0 until maxMetrics)
                     .map(i => get_json_object($"records", s"$$[$i]"))


val newDF = eventhubs
  .withColumn("records", explode(array(jsonElements: _*))) // Here _* is a special expression in spark to get each element of the array
  .where(!isnull($"records")) 


// from_json - Parses a column containing a JSON string into a MapType with StringType as keys type, StructType or ArrayType with the specified schema
// Now we need to convert each json string to a json object with a defined schema

val dataSchema = new StructType()
        .add("count", LongType)
        .add("total", LongType)
        .add("minimum", LongType)
        .add("maximum", LongType)
        .add("resourceId", StringType)
        .add("time", DataTypes.DateType)
        .add("metricName", StringType)
        .add("timeGrain", StringType)
        .add("average", LongType)

val df=newDF.withColumn("records",from_json(col("records"),dataSchema))

// Next we need to ensure there are multiple columns for each property of the JSON object
val finalDF=df.select(col("records.*"))

val tablename="dblog"
val tmpdir="abfss://tmpdir@datalake2000.dfs.core.windows.net/log"

// This is the connection to our Azure Synapse dedicated SQL pool
val connection = "jdbc:sqlserver://appworkspace9000.sql.azuresynapse.net:1433;database=newpool;user=sqladminuser;password=Azure@123;encrypt=true;trustServerCertificate=false;"

// We can use the write function to write to an external data store
finalDF.writeStream// Here we need to change the function as writeStream to now write our stream to Azure Synapse  
  .format("com.databricks.spark.sqldw")
  .option("url", connection)
  .option("tempDir", tmpdir) // For transfering to Azure Synapse, we need temporary storage for the staging data
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tablename)
  .option("checkpointLocation","/tmp_location") // We need to mention a checkpoint location  
  /*
  The checkpoint helps to resume a query from where it left off, if the query fails for any reason 
  in the middle of processing data.
  Each query should have a different checkpoint location
  */
  .start()
