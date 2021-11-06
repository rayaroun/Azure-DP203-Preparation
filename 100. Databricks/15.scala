// Lab - Using the PIVOT command

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.Date

val schema = StructType( Array(
                 StructField("Date", DataTypes.DateType),
                 StructField("Temp", IntegerType)
             ))

val dataRows=Seq(
    Row(Date.valueOf("2021-01-18"),3),
    Row(Date.valueOf("2021-01-19"),4),
    Row(Date.valueOf("2021-01-20"),2),
    Row(Date.valueOf("2021-01-21"),2),
    Row(Date.valueOf("2021-02-18"),5),
    Row(Date.valueOf("2021-02-19"),6),
    Row(Date.valueOf("2021-02-20"),7),
    Row(Date.valueOf("2021-02-21"),8),
    Row(Date.valueOf("2021-03-18"),8),
    Row(Date.valueOf("2021-03-19"),9),
    Row(Date.valueOf("2021-03-20"),10),
    Row(Date.valueOf("2021-03-21"),10),
    Row(Date.valueOf("2021-04-18"),11),
    Row(Date.valueOf("2021-04-19"),12),
    Row(Date.valueOf("2021-04-20"),13),
    Row(Date.valueOf("2021-04-21"),14),
    Row(Date.valueOf("2021-05-18"),14),
    Row(Date.valueOf("2021-05-19"),14),
    Row(Date.valueOf("2021-05-20"),15),
    Row(Date.valueOf("2021-05-21"),15),
    Row(Date.valueOf("2021-06-18"),16),
    Row(Date.valueOf("2021-06-19"),15),
    Row(Date.valueOf("2021-06-20"),17),
    Row(Date.valueOf("2021-06-21"),18),
    Row(Date.valueOf("2021-07-18"),20),
    Row(Date.valueOf("2021-07-19"),21),
    Row(Date.valueOf("2021-07-20"),22),
    Row(Date.valueOf("2021-07-21"),23),
    Row(Date.valueOf("2021-08-18"),26),
    Row(Date.valueOf("2021-08-19"),28),
    Row(Date.valueOf("2021-08-20"),30),
    Row(Date.valueOf("2021-08-21"),35),
    Row(Date.valueOf("2021-08-18"),37),
    Row(Date.valueOf("2021-09-19"),40),
    Row(Date.valueOf("2021-09-20"),43),
    Row(Date.valueOf("2021-09-21"),44),
    Row(Date.valueOf("2021-09-18"),57),
    Row(Date.valueOf("2021-10-19"),30),
    Row(Date.valueOf("2021-10-20"),20),
    Row(Date.valueOf("2021-11-21"),25),
    Row(Date.valueOf("2021-11-18"),15),
    Row(Date.valueOf("2021-12-19"),13),
    Row(Date.valueOf("2021-12-20"),12)
    
)

val newdf=spark.createDataFrame(sc.parallelize(dataRows),schema)

newdf.write.mode("overwrite").saveAsTable("temperatures")
display(newdf)



%sql
SELECT * FROM (
SELECT YEAR(Date) Year, MONTH(Date) Month, Temp
FROM temperatures
WHERE date BETWEEN Date '2019-01-01' AND DATE '2021-12-31')
PIVOT(AVG(CAST(Temp AS FLOAT))
FOR Month in 
(1 JAN, 2 FEB, 3 MAR, 4 APR, 5 MAY, 6 JUN, 7 JUL, 8 AUG, 9 SEP, 10 OCT, 11 NOV, 12 DEC)) ORDER BY YEAR ASC