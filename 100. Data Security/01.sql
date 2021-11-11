-- Lab - Azure Synapse - Data Masking

CREATE USER UserA WITHOUT LOGIN;  

GRANT SELECT ON [Person].[EmailAddress] TO UserA; 

EXECUTE AS USER = 'UserA';  
SELECT * FROM [Person].[EmailAddress];
REVERT;  
