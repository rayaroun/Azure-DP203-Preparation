//Lab - Removing duplicate rows

import org.apache.spark.sql.functions._

spark.conf.set(
    "fs.azure.account.key.datalake2000.dfs.core.windows.net",
    dbutils.secrets.get(scope="data-lake-key",key="datalake2000"))

val df = spark.read.format("csv")
.options(Map("inferSchema"->"true","header"->"true"))
.load("abfss://data@datalake2000.dfs.core.windows.net/raw/Log_withduplicates.csv")

display(df.groupBy(df("Id")).count().orderBy(df("Id")))

// Remove duplicates


val df1=df.dropDuplicates()
display(df1.groupBy(df1("Id")).count().orderBy(df1("Id")))


// If you want to remove duplicates based on specific columns

val df2=df.dropDuplicates("Id")
display(df2.groupBy(df2("Id")).count().orderBy(df2("Id")))
