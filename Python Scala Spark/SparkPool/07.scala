// Lab - Spark Pool - Creating tables

// The tables are basically Parquet-backed tables
// This can be done with the SQL command
// The datetime format is not available
// Data types - https://docs.microsoft.com/en-us/azure/synapse-analytics/metadata/table#expose-a-spark-table-in-sql
%%sql
CREATE DATABASE internaldb
CREATE TABLE internaldb.customer(Id int,name varchar(200)) USING Parquet

%%sql
INSERT INTO internaldb.customer VALUES(1,'UserA')

%%sql
SELECT * FROM internaldb.customer


// If you want to load data from the log.csv file and then save to a table
%%pyspark
df = spark.read.load('abfss://data@datalake2000.dfs.core.windows.net/raw/Log.csv', format='csv'
, header=True
)
df.write.mode("overwrite").saveAsTable("internaldb.logdatanew")

%%sql
SELECT * FROM internaldb.logdatanew
