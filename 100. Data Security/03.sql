-- Lab - Azure Synapse - Row-Level Security

CREATE TABLE [dbo].[Orders] 
(  
    OrderID int,  
    Agent varchar(50),  
    Course varchar(50),  
    Quantity int  
);  

-- Insert rows into the table

INSERT INTO [dbo].[Orders] VALUES(1,'AgentA','AZ-900',5);
INSERT INTO [dbo].[Orders] VALUES(1,'AgentA','DP-203',4);
INSERT INTO [dbo].[Orders] VALUES(1,'AgentB','AZ-104',5);
INSERT INTO [dbo].[Orders] VALUES(1,'AgentB','AZ-303',6);
INSERT INTO [dbo].[Orders] VALUES(1,'AgentA','AZ-304',7);
INSERT INTO [dbo].[Orders] VALUES(1,'AgentB','DP-900',8);

-- Create three database users

CREATE USER Supervisor WITHOUT LOGIN;  
CREATE USER AgentA WITHOUT LOGIN;  
CREATE USER AgentB WITHOUT LOGIN;  

-- Grant access to the tables for the users

GRANT SELECT ON [dbo].[Orders] TO Supervisor; 
GRANT SELECT ON [dbo].[Orders] TO AgentA; 
GRANT SELECT ON [dbo].[Orders] TO AgentB; 

-- Create a new schema for the security function

CREATE SCHEMA Security;  

-- Create an inline table function
-- The function returns 1 when a row in the Agentcolumn is the same as the user executing the query 
-- (@Agent = USER_NAME()) or if the user executing the query is the Manager user (USER_NAME() = 'Supervisor').

CREATE FUNCTION Security.securitypredicate(@Agent AS nvarchar(50))  
    RETURNS TABLE  
WITH SCHEMABINDING  
AS  
    RETURN SELECT 1 AS securitypredicate_result
WHERE @Agent = USER_NAME() OR USER_NAME() = 'Supervisor';  

-- Create a security policy adding the function as a filter predicate. The state must be set to ON to enable the policy.

CREATE SECURITY POLICY Filter  
ADD FILTER PREDICATE Security.securitypredicate(Agent)
ON [dbo].[Orders] 
WITH (STATE = ON);  
GO

-- Lab - Azure Synapse - Row-Level Security

-- Allow SELECT permissions to the function

GRANT SELECT ON Security.securitypredicate TO Supervisor;
GRANT SELECT ON Security.securitypredicate TO AgentA;  
GRANT SELECT ON Security.securitypredicate TO AgentB;  

-- Test is for the different users

EXECUTE AS USER = 'AgentA';  
SELECT * FROM [dbo].[Orders];
REVERT;  
  
EXECUTE AS USER = 'AgentB';  
SELECT * FROM [dbo].[Orders];
REVERT;  
  
EXECUTE AS USER = 'Supervisor';  
SELECT * FROM [dbo].[Orders];
REVERT; 

-- Drop all of the artefacts

DROP USER Supervisor;
DROP USER AgentA;
DROP USER AgentB;

DROP SECURITY POLICY Filter;
DROP TABLE [dbo].[Orders];
DROP FUNCTION Security.securitypredicate;
DROP SCHEMA Security;




