# Lab - Spark Pool - Load data

from pyspark.sql import SparkSession
from pyspark.sql.types import *

account_name = "datalake2000"
container_name = "data"
relative_path = "raw"
adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name, relative_path)

spark.conf.set("fs.azure.account.auth.type.%s.dfs.core.windows.net" %account_name, "SharedKey")
spark.conf.set("fs.azure.account.key.%s.dfs.core.windows.net" %account_name ,"V0bi0fr1nxs3Ox4fbPUGNDtE5XlGqvYT9tJJt0hkYS2ncXmiJtcW5DO2OLzffWKLQ410oITK3Ra7xN9Qjn1hhA==")

df1 = spark.read.option('header', 'true') \
                .option('delimiter', ',') \
                .csv(adls_path + '/Log.csv')

display(df1)

# The Spark groupBy function is used to collect identical data and segregate them into groups
# Then you can perform aggregation on the grouped data

from pyspark.sql.functions import *

# .agg is a method that can be used to perform aggregation based on a column of data
# .orderBy helps to order by a particular column
newdf=(df1.groupBy("Operationname")
     .agg(count("Correlationid").alias("Total operations"))
     .orderBy(col("Total operations").desc()))

display(newdf)