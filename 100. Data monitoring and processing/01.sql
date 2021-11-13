-- Azure Stream Analytics - An example on monitoring the stream analytics job

CREATE TABLE [dbo].[NetworkFlow]
(
	[time] datetimeoffset,
	[SourceIP] varchar(15),
        [SourcePort] varchar(5),
	[Direction] varchar(1),
	[Action] varchar(1)
)