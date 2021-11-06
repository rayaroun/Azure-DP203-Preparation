import org.apache.spark.sql.functions._

spark.conf.set(
    "fs.azure.account.key.datalake2000.dfs.core.windows.net",
    dbutils.secrets.get(scope="data-lake-key",key="datalake2000"))

val df = spark.read.format("csv")
.options(Map("inferSchema"->"true","header"->"true"))
.load("abfss://data@datalake2000.dfs.core.windows.net/raw/Log.csv")

val dfcorrect=df.select(col("Id"),
                        col("Correlationid"),
                        col("Operationname"),
                        col("Status"),
                        col("Eventcategory"),
                        col("Level"),
                        col("Time"),
                        col("Subscription"),
                        col("Eventinitiatedby"),
                        col("Resourcetype"),
                        col("Resourcegroup"))


val tablename="logdata"
val tmpdir="abfss://tmpdir@datalake2000.dfs.core.windows.net/log"

// This is the connection to our Azure Synapse dedicated SQL pool
val connection = "jdbc:sqlserver://appworkspace9000.sql.azuresynapse.net:1433;database=newpool;user=sqladminuser;password=Azure@123;encrypt=true;trustServerCertificate=false;"

// We can use the write function to write to an external data store
dfcorrect.write
  .mode("append") // Here we are saying to append to the table
  .format("com.databricks.spark.sqldw")
  .option("url", connection)
  .option("tempDir", tmpdir) // For transfering to Azure Synapse, we need temporary storage for the staging data
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tablename)
  .save()
