

-- Here we want to create an external table based on the Azure AD user credentials.

CREATE EXTERNAL DATA SOURCE log_dataAD
WITH (    LOCATION   = 'abfss://data@datalake2000.dfs.core.windows.net',
          TYPE = HADOOP
)


CREATE EXTERNAL TABLE [logdata]
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
WITH (
 LOCATION = 'cleaned/Log.csv',
    DATA_SOURCE = log_dataAD,  
    FILE_FORMAT = TextFileFormat
)



SELECT * FROM logdata


