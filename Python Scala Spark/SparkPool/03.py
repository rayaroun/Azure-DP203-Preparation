# Lab - Spark Pool - Removing NULL values

filterdf=newdf.filter(col("Operationname").isNotNull())
display(filterdf)