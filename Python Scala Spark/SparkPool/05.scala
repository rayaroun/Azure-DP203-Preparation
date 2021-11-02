// Lab - Spark Pool - Write data to Azure Synapse

// There is a connector between Azure Synapse Spark Pool and Azure Synapse Dedicated SQL Pool
// You can write data directly onto the Dedicated SQL Pool

// Here we are saying to use Spark Scala because this functionality only works in Scala
%%Spark

// The following libraries need to be imported
import com.microsoft.spark.sqlanalytics.utils.Constants
import org.apache.spark.sql.SqlAnalyticsConnector._


/* First read the data from the Log.csv file

 We are defining the schema first for the data in the file

 This is because the table is going to be created automatically by our write command

 And for that we want the data types of the columns to correctly reflect the data.

 You can also infer the schema

 */

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


val dataSchema = StructType(Array(
    StructField("Id", IntegerType, true),
    StructField("Correlationid", StringType, true),
    StructField("Operationname", StringType, true),
    StructField("Status", StringType, true),
    StructField("Eventcategory", StringType, true),
    StructField("Level", StringType, true),
    StructField("Time", TimestampType, true),
    StructField("Subscription", StringType, true),
    StructField("Eventinitiatedby", StringType, true),
    StructField("Resourcetype", StringType, true),
    StructField("Resourcegroup", StringType, true)))


val df = spark.read.format("csv").option("header","true").schema(dataSchema).load("abfss://data@datalake2000.dfs.core.windows.net/raw/Log.csv")

df.printSchema()

/* 

1. Here the table must not exist

2. Here we will be transfering data in the context of the Azure Admin user - techsup1000@gmail.com
This user must be added as an Azure AD user in Azure Synapse
Also the same user must have the 'Storage Blob Contributor Role' on the storage account attached to the Synapse workspace

*/
/*The following can be used to read data from a table in the dedicated SQL pool
spark.read.sqlanalytics("<DBName>.<Schema>.<TableName>")

DBName: the name of the database.
Schema: the schema definition such as dbo.
TableName: the name of the table you want to read data from.
*/

/*The following can be used to write data to a table in the dedicated SQL pool
df.write.sqlanalytics("<DBName>.<Schema>.<TableName>", <TableType>)

DBName: the name of the database.
Schema: the schema definition such as dbo.
TableName: the name of the table you want to read data from.
TableType: specification of the type of table, which can have two values.
Constants.INTERNAL - Managed table in dedicated SQL pool
Constants.EXTERNAL - External table in dedicated SQL pool

*/

import com.microsoft.spark.sqlanalytics.utils.Constants
import org.apache.spark.sql.SqlAnalyticsConnector._
df.write.
sqlanalytics("newpool.dbo.logdata", Constants.INTERNAL)