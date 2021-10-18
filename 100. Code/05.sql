-- Lab - Loading data into a table - COPY Command - Parquet

DROP TABLE [logdata]

-- Recreate the table
-- Here again I am using the data type with MAX because that is how I generated the parquet files when it came to the data type
-- Here we need to specify the clustered index based on a column , because indexes can't be created on varchar(MAX)

CREATE TABLE [logdata]
(
        [Id] [int] NULL,
	[Correlationid] [varchar](200) NULL,
	[Operationname] [varchar](200) NULL,
	[Status] [varchar](100) NULL,
	[Eventcategory] [varchar](100) NULL,
	[Level] [varchar](100) NULL,
	[Time] [datetime] NULL,
	[Subscription] [varchar](200) NULL,
	[Eventinitiatedby] [varchar](1000) NULL,
	[Resourcetype] [varchar](1000) NULL,
	[Resourcegroup] [varchar](1000) NULL
)

-- Grant the permissions again. Because when you drop the table, you also drop the associated permission

GRANT INSERT ON logdata TO user_load;
GRANT SELECT ON logdata TO user_load;

COPY INTO [logdata] FROM 'https://appdatalake7000.blob.core.windows.net/data/parquet/*.parquet'
WITH
(
FILE_TYPE='PARQUET',
CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='sv=2020-02-10&ss=b&srt=sco&sp=rl&se=2021-07-01T16:07:07Z&st=2021-07-01T08:07:07Z&spr=https&sig=j%2BtdThwbGU83Ol3LyyLHbFZQTMyGauCVtfKbUuUCkLM%3D')
)

SELECT * FROM [logdata]