// Lab - The SQL Data Frame

val df2 = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/shared_uploads/techsup1000@gmail.com/Log.csv")

// Select certain columns
df2.select("Operation name","Status").show()

// Now we are asking Spark to infer the data type
// Now the dataset will take Id as an Integer and Time as timestamp
val df2 = spark.read.format("csv").options(Map("inferSchema"->"true","header"->"true")).load("dbfs:/FileStore/shared_uploads/techsup1000@gmail.com/Log.csv")

// If you want to filter out just the rows that contain the value of Succeeded for the Status
// There are different ways to access the column value, below is one way
df2.filter(df2("Status")==="Succeeded").show()

// OR display(df2.filter(df2.col("Status")==="Succeeded"))

// The same method but using display now
display(df2.filter(df2("Status")==="Succeeded"))

// If you want to group by Status
display(df2.groupBy(df2("Status")).count())