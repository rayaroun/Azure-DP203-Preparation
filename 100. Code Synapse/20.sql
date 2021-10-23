-- Lab - Switching partitions

-- Create a new table with partitions
-- Switch partitions
-- This can be done with the Alter command. 
-- But the alter command will not work if the table has a clustered column store index
-- When using the CREATE TABLE AS , we need to mention a distribution type


CREATE TABLE [logdata_new]
WITH
(
DISTRIBUTION = ROUND_ROBIN,
PARTITION ( [Time] RANGE RIGHT FOR VALUES
            ('2021-05-01','2021-06-01')

   ) ) 
AS
SELECT * 
FROM logdata
WHERE 1=2

-- Switch the partitions and then see the data

ALTER TABLE [logdata] SWITCH PARTITION 2 TO [logdata_new] PARTITION 1;

SELECT count(*) FROM [logdata_new]
SELECT FORMAT(Time,'yyyy-MM-dd') AS dt,COUNT(*) FROM logdata_new
GROUP BY FORMAT(Time,'yyyy-MM-dd')

-- See the data in the original table

SELECT FORMAT(Time,'yyyy-MM-dd') AS dt,COUNT(*) FROM logdata
GROUP BY FORMAT(Time,'yyyy-MM-dd')


