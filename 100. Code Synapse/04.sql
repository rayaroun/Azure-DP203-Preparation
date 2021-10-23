-- Lab - Loading data into a table - COPY Command - CSV

-- Never use the admin account for load operations.
-- Create a seperate user for load operations


-- This has to be run in the master database
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

-- Drop the external table if it exists
DROP EXTERNAL TABLE logdata

-- Create a normal table
-- Login as the new user and create the table
-- Here I have added more constraints when it comes to the width of the data type

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

-- Grant the required privileges to the new user

GRANT INSERT ON logdata TO user_load;
GRANT SELECT ON logdata TO user_load;

-- Now log in as the new user
-- The FIRSTROW option helps to ensure the first header row is not part of the COPY implementation
-- https://docs.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=azure-sqldw-latest&preserve-view=true


SELECT * FROM [logdata]

-- Here there is no authentication/authorization, so you need to allow public access for the container
COPY INTO logdata FROM 'https://appdatalake7000.blob.core.windows.net/data/Log.csv'
WITH
(
FIRSTROW=2
)




