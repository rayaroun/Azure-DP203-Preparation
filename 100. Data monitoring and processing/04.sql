-- Azure Stream Analytics - More on the time aspect

-- EventProcessedUtcTime	The date and time that the event was processed by Stream Analytics.
-- EventEnqueuedUtcTime	The date and time that the event was received by Event Hubs.


SELECT
    Records.ArrayValue.count as [count],
    Records.ArrayValue.total as [total],
    Records.ArrayValue.minimum as [minimum],
    Records.ArrayValue.minimum as [maximum],
    Records.ArrayValue.resourceId as [resourceId],
    CAST(Records.ArrayValue.time AS datetime) as [time],
    System.Timestamp as systemtime,
    d.EventProcessedUtcTime as eventime,
    Records.ArrayValue.metricName as [metricName],
    Records.ArrayValue.timeGrain as [timeGrain],
    Records.ArrayValue.average as [average]
    
INTO
    dblog
FROM
    dbmultihub d
    CROSS APPLY GetArrayElements(d.records) AS Records 