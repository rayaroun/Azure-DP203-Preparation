// Lab - Few functions on dates
// If want to start using date/time functions

// Here we are getting the year,month and day of year
import org.apache.spark.sql.functions._
display(df2.select(year(col("time")),month(col("time")),dayofyear(col("time"))))

// Use alias if you want to give more meaningful names to the output columns
display(df2.select(year(col("time")).alias("Year"),month(col("time")).alias("Month"),dayofyear(col("time")).alias("Day of Year")))

// If you want to convert the date to a particular format
display(df2.select(to_date(col("time"),"dd-mm-yyyy").alias("Date")))

