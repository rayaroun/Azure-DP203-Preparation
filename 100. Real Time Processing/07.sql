-- Lab - OVER clause

WITH
staginglogs AS
(
SELECT
Records.ArrayValue.total AS [total],
Records.ArrayValue.metricName AS [metricName]
FROM
    dbhub d
    CROSS APPLY GetArrayElements(d.records) AS Records
),
newlogs AS
(
SELECT total,metricName FROM staginglogs
WHERE metricName='Storage'
)

SELECT AVG(total) OVER(LIMIT DURATION(minute,5)) 
FROM newlogs 


WITH
staginglogs AS
(
SELECT
Records.ArrayValue.total AS [total],
Records.ArrayValue.metricName AS [metricName]
FROM
    dbhub d
    CROSS APPLY GetArrayElements(d.records) AS Records
),
newlogs AS
(
SELECT total,metricName FROM staginglogs
WHERE metricName='Storage'
)

SELECT AVG(total) OVER(LIMIT DURATION(minute,5)) 
FROM newlogs 


-- With LAG operator
SELECT total,change=total-LAG(total) OVER(LIMIT DURATION(minute,5)) 

-- With LAST operator
SELECT total,LAST(System.Timestamp()) OVER(LIMIT DURATION(minute,5) WHEN total>0) 




