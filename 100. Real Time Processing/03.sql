-- Lab - Reading data from the Event Hub - Implementation

-- Reference
-- https://docs.microsoft.com/en-us/stream-analytics-query/getarrayelements-azure-stream-analytics

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
    dbhub d
    CROSS APPLY GetArrayElements(d.records) AS Records 