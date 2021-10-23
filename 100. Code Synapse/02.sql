-- Lab - SQL Pool - External Tables - CSV

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'P@ssw0rd@123';

-- Here we are using the Storage account key for authorization

CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
WITH
  IDENTITY = 'appdatalake7000',
  SECRET = 'VqJnhlUibasTfhSuAxkgIgY97GjRzHL9VNOPkjD8y+KYzl1LSDCflF6LXlrezAYKL3Mf1buLdZoJXa/38BXLYA==';

-- In the SQL pool, we can use Hadoop drivers to mention the source

CREATE EXTERNAL DATA SOURCE log_data
WITH (    LOCATION   = 'abfss://data@appdatalake7000.dfs.core.windows.net',
          CREDENTIAL = AzureStorageCredential,
          TYPE = HADOOP
)

CREATE EXTERNAL FILE FORMAT TextFileFormat WITH (  
      FORMAT_TYPE = DELIMITEDTEXT,  
    FORMAT_OPTIONS (  
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2))


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
 LOCATION = '/Log.csv',
    DATA_SOURCE = log_data,  
    FILE_FORMAT = TextFileFormat
)



SELECT * FROM logdata


SELECT [Operation name] , COUNT([Operation name]) as [Operation Count]
FROM logdata
GROUP BY [Operation name]
ORDER BY [Operation Count]