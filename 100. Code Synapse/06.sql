-- Lab - Loading data using PolyBase

CREATE LOGIN user_load WITH PASSWORD = 'Azure@123';

CREATE USER user_load FOR LOGIN user_load;
GRANT ADMINISTER DATABASE BULK OPERATIONS TO user_load;
GRANT CREATE TABLE TO user_load;
GRANT ALTER ON SCHEMA::dbo TO user_load;

CREATE WORKLOAD GROUP DataLoads
WITH ( 
    MIN_PERCENTAGE_RESOURCE = 100
    ,CAP_PERCENTAGE_RESOURCE = 100
    ,REQUEST_MIN_RESOURCE_GRANT_PERCENT = 100
    );

CREATE WORKLOAD CLASSIFIER [ELTLogin]
WITH (
        WORKLOAD_GROUP = 'DataLoads'
    ,MEMBERNAME = 'user_load'
);

-- Here we are following the same process of creating an external table

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'P@ssw0rd@123' ;

-- If you want to see existing database scoped credentials
SELECT * FROM sys.database_scoped_credentials

CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
WITH
  IDENTITY = 'appdatalake7000',
  SECRET = 'VqJnhlUibasTfhSuAxkgIgY97GjRzHL9VNOPkjD8y+KYzl1LSDCflF6LXlrezAYKL3Mf1buLdZoJXa/38BXLYA==';

-- If you want to see the external data sources
SELECT * FROM sys.external_data_sources 


CREATE EXTERNAL DATA SOURCE log_data
WITH (    LOCATION   = 'abfss://data@appdatalake7000.dfs.core.windows.net',
          CREDENTIAL = AzureStorageCredential,
          TYPE = HADOOP
)

-- If you want to see the external file formats

SELECT * FROM sys.external_file_formats

CREATE EXTERNAL FILE FORMAT parquetfile  
WITH (  
    FORMAT_TYPE = PARQUET,  
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'  
);


-- Drop the existing logdata table
DROP TABLE [logdata]

-- Create the external table as the admin user

CREATE EXTERNAL TABLE [logdata_external]
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
 LOCATION = '/parquet/',
    DATA_SOURCE = log_data,  
    FILE_FORMAT = parquetfile
)

-- Now create a normal table by selecting all of the data from the external table

CREATE TABLE [logdata]
WITH
(
DISTRIBUTION = ROUND_ROBIN,
CLUSTERED INDEX (id)   
)
AS
SELECT  *
FROM  [logdata_external];








