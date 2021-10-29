-- Lab - Adding multiple outputs

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

SELECT staginglogs.metricName AS [metricName],COUNT(*) AS [Count],MAX(time) AS [TimeStamp]
INTO
    summary 	
FROM
    staginglogs
GROUP BY staginglogs.metricName,TumblingWindow(second,10)        

SELECT
    Records.ArrayValue.count as [count],
    Records.ArrayValue.total as [total],
    Records.ArrayValue.minimum as [minimum],
    Records.ArrayValue.minimum as [maximum],
    Records.ArrayValue.resourceId as [resourceId],
    CAST(Records.ArrayValue.time AS datetime) as [time],
    Records.ArrayValue.metricName as [metricName],
    Records.ArrayValue.timeGrain as [timeGrain],
    Records.ArrayValue.average as [average]
INTO
    OrderSynapse
FROM
    dbhuball d
    CROSS APPLY GetArrayElements(d.records) AS Records 