-- Lab - Azure Synapse - External Tables Authorization via Managed Identity

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'P@ssw0rd@123';

-- Here we are now using the managed identity

CREATE DATABASE SCOPED CREDENTIAL AzureManaged
WITH IDENTITY = 'Managed Identity'

-- In the SQL pool, we can use Hadoop drivers to mention the source

CREATE EXTERNAL DATA SOURCE log_data_managed
WITH (    LOCATION   = 'abfss://data@datalake2000.dfs.core.windows.net',
          CREDENTIAL = AzureManaged,
          TYPE = HADOOP
)


CREATE EXTERNAL FILE FORMAT TextFileFormatManaged WITH (  
      FORMAT_TYPE = DELIMITEDTEXT,  
    FORMAT_OPTIONS (  
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2))


CREATE EXTERNAL TABLE logdatamanaged
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
    DATA_SOURCE = log_data_managed,  
    FILE_FORMAT = TextFileFormatManaged
)



SELECT * FROM logdatamanaged

-- If you want to clean up your resources

DROP EXTERNAL TABLE logdatamanaged
DROP EXTERNAL FILE FORMAT TextFileFormatManaged
DROP EXTERNAL DATA SOURCE log_data_managed
DROP DATABASE SCOPED CREDENTIAL AzureManaged

