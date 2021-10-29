-- Lab - Reference data

WITH
staginglogs AS
(
SELECT
Records.ArrayValue.metricName AS [metricName],
CAST(Records.ArrayValue.time AS datetime) AS [time]
FROM
    dbhub d TIMESTAMP BY EventEnqueuedUtcTime
    CROSS APPLY GetArrayElements(d.records) AS Records
)

SELECT s.metricName AS [metricName],MAX(s.time) AS [TimeStamp],COUNT(*) AS [Count] 
INTO
    summary 	
FROM staginglogs s
JOIN tier t
ON t.metricName=s.metricName
WHERE t.tier='2'
GROUP BY s.metricName,TumblingWindow(minute,5)      