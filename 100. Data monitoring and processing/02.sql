-- Azure Stream Analytics - An example on multiple partitions

SELECT
    Records.ArrayValue.count as [count],
    Records.ArrayValue.total as [total],
    Records.ArrayValue.minimum as [minimum],
    Records.ArrayValue.minimum as [maximum],
    Records.ArrayValue.resourceId as [resourceId],
    CAST(Records.ArrayValue.time AS datetime) as [time],
    Records.ArrayValue.metricName as [metricName],
    Records.ArrayValue.timeGrain as [timeGrain],
     CAST(Records.ArrayValue.average AS bigint) as [average] 
INTO
    dblog
FROM
    dbmultihub d
    CROSS APPLY GetArrayElements(d.records) AS Records 