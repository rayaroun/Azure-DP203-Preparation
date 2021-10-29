-- Lab - Reading data from a JSON file - Implementation

CREATE TABLE [dbo].[dblog]
(
	[count] [bigint],
	[total] [bigint],
	[minimum] [bigint],
	[maximum] [bigint],
	[resourceId] [varchar](1000),
	[time] datetime,
	[metricName] [varchar](500),
	[timeGrain] [varchar](100),
	[average] [bigint]
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)


SELECT
    [count],[total],[minimum],[maximum],[resourceId],[time],[metricName],[timeGrain],[average]
INTO
    OrderSynapse
FROM
    datastore