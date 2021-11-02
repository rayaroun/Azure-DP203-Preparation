
# Lab - Spark Pool - Using SQL statements
# First register the data frame as a temporary view

df1.createOrReplaceTempView("logdata")

sql_1=spark.sql("SELECT Operationname, count(Operationname) FROM logdata GROUP BY Operationname")
sql_1.show()

# OR

%%sql
SELECT Operationname, count(Operationname) FROM logdata GROUP BY Operationname