-- Lab - Timing windows

-- Here We want to group the events by metricName and see the count of events fired every 2 minutes
-- Also we will have multiple inputs and outputs
-- https://docs.microsoft.com/en-us/stream-analytics-query/with-azure-stream-analytics

CREATE TABLE [Summary]
(
	[metricName] [varchar](500) NULL,
	[Count] int,
	[TimeStamp] datetime
)

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