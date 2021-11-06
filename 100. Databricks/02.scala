// Lab - Reading the CSV file
// Load the csv file

val df1 = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/techsup1000@gmail.com/Log.csv")

df1.show()

// The display command can be used in the notebook
// This provides a more pleasant approach when it comes to displaying data

display(df1)

// To see the underlying data types
df1.printSchema()

// Here we want are telling to take our first row as the header row
val df2 = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/shared_uploads/techsup1000@gmail.com/Log.csv")
display(df2)