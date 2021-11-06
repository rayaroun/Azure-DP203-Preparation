// Lab - Creating a Delta Lake Table

import org.apache.spark.sql.functions._

spark.conf.set(
    "fs.azure.account.key.datalake2000.dfs.core.windows.net",
    dbutils.secrets.get(scope="data-lake-key",key="datalake2000"))

val df = spark.read.format("json")
.options(Map("inferSchema"->"true","header"->"true"))
.load("abfss://data@datalake2000.dfs.core.windows.net/raw/PT1H.json")

// Create a delta lake table

df.write.format("delta").mode("overwrite").saveAsTable("metrics")

%sql

SELECT * FROM metrics

// If you want to speed up queries that make use of predicates that involve partition columns
// If you let's say use the metric name in where conditions

df.write.partitionBy("metricName").format("delta").mode("overwrite").saveAsTable("partitionedmetrics")

%sql

SELECT metricName,count(*) FROM partitionedmetrics
GROUP BY metricName
