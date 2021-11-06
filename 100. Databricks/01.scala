// Lab - Using Data Frames
// The DataFrame class is part of org.apache.spark.sql 

// First we are using the Seq trait to create a sequence
// In the sequence , for each collection, we have details of a course
val data=Seq((1,"DP-203",9.99),(1,"AI-102",10.99),(1,"AZ-204",11.99))

// We can then create an RDD from the sequence
val dataRDD=sc.parallelize(data)

// From the output of the RDD , you will see the data types are being automatically inferred

// Then we can convert the RDD to a data frame
val df=dataRDD.toDF()
display(df)

// ===============================================================
// We can also specify the column names and data type specifically when creating the data frame


// The import statement is needed to reference the Types in the StructField class
import org.apache.spark.sql.types._

// Reference for the data types - https://spark.apache.org/docs/1.6.0/api/java/org/apache/spark/sql/types/DataTypes.html
val schema = StructType( Array(
                 StructField("Course ID", IntegerType),
                 StructField("Course Name", StringType),
                 StructField("Course price", DoubleType),
             ))

// Here our data needs to be of the Row type
val dataRows=Seq(
    Row(1,"DP-203",9.99),
    Row(2,"AI-102",10.99),
    Row(3,"AZ-204",11.99))

val newdf=spark.createDataFrame(sc.parallelize(dataRows),schema)
display(newdf)

// ===============================================================

// If you want to sort by price descending

val sortdf=newdf.sort(newdf.col("Course price").desc)
display(sortdf)

// Filtering with the where condition

import org.apache.spark.sql.functions._
val filterdf=newdf.where($"Course Name"==="DP-203")
display(filterdf)

// Using aggregate functions

val priceavg=newdf.agg("Course price"->"avg")
display(priceavg)