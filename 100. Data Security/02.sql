-- Lab - Azure Synapse - Azure AD Authentication - Creating a user

CREATE USER [newsql@techsup1000gmail.onmicrosoft.com]
FROM EXTERNAL PROVIDER 
WITH DEFAULT_SCHEMA = dbo;

CREATE ROLE [readrole]
GRANT SELECT ON SCHEMA::[dbo] TO [readrole]
EXEC sp_addrolemember N'readrole', N'newsql@techsup1000gmail.onmicrosoft.com'

SELECT * FROM [dbo].[DimCustomer]