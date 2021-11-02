// Lab - Spark Pool - JSON files

%%spark

val df = spark.read.format("json").load("abfss://data@datalake2000.dfs.core.windows.net/raw/customer/customer_arr.json")
display(df)

// Now we need to expand the courses information

%%spark
import org.apache.spark.sql.functions._
val df = spark.read.format("json").load("abfss://data@datalake2000.dfs.core.windows.net/raw/customer/customer_arr.json")
val newdf=df.select(col("customerid"),col("customername"),col("registered"),explode(col("courses")))
display(newdf)

// Reading the customer object file
%%spark
import org.apache.spark.sql.functions._
val df = spark.read.format("json").load("abfss://data@datalake2000.dfs.core.windows.net/raw/customer/customer_obj.json")
val newdf=df.select(col("customerid"),col("customername"),col("registered"),explode(col("courses")),col("details.city"),col("details.mobile"))
display(newdf)





