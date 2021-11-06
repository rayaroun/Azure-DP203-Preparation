// Lab - JSON-based files

val dfjson = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/techsup1000@gmail.com/customer_arr.json")

// Here you will see the array elements are being shown as one data item
display(dfjson)

// Use the explode function

val newjson=dfjson.select(col("customerid"),col("customername"),col("registered"),explode(col("courses")))

display(newjson)

// If you get an error that Spark cannot find the function get_json_object, you are missing the import statement
import org.apache.spark.sql.functions._
val dfobj = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/techsup1000@gmail.com/customer_obj.json")
// Here we are showing how to access elements of a nested JSON object
display(dfobj.select(col("customerid"),col("customername"),col("registered"),(col("details.city")),(col("details.mobile")),explode(col("courses")).alias("Courses")))


