
// Spark Pool - Creating tables
// Here we are creating a tables in the metastore of the Spark pool

%%spark
val df = spark.read.sqlanalytics("newpool.dbo.logdata") 
df.write.mode("overwrite").saveAsTable("logdatainternal")

// Then we can reference the table via SQL commands

%%sql

SELECT * FROM logdatainternal