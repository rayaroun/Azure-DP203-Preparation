// Lab - Parquet-based files

// Here we will open a parquet based file

val dfparquet = spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/techsup1000@gmail.com/Log.parquet")

// Perform the same operations as would normally perform on data frames
dfparquet.printSchema()

display(dfparquet)

df2.filter(df2("Status")==="Succeeded").show()

display(df2.filter(col("Resource group").isNull))