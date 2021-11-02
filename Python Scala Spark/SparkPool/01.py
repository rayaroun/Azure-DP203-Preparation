
# Lab - Spark Pool - Spark DataFrames
# Here we are creating a data frame

courses = [(1,'AZ-900',10.99),(2,'DP-203',11.99) ,(3,'AZ-104',12.99)]
df = spark.createDataFrame(courses, ['Id', 'Name', 'Price'])
df.show()

# Then here we are descending the rows based on the value in the price column

from pyspark.sql.functions import desc
from pyspark.sql.functions import col

# Here col is a class that is used to represent a new column that will be constructed based on the input column data
# You can use different variations here , col("columnname") or $"columnname", df("columnname")

sorteddf=df.sort(col("Price").desc())
sorteddf.show()