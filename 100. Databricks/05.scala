// Lab - Filtering on NULL values
// Seeing all the rows where the resource group is NULL

display(df2.filter(col("Resource group").isNull))

// Getting all rows which don't have null values for resource group and assign it to a data frame
val df3=df2.filter(col("Resource group").isNotNull)


df2.count() 
df3.count() 