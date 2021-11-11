-- Lab - Azure Synapse - Column-Level Security

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
CREATE USER UserA WITHOUT LOGIN;  

-- Grant access to the tables for the users

GRANT SELECT ON [dbo].[Orders] TO Supervisor; 
GRANT SELECT ON [dbo].[Orders](OrderID,Course,Quantity) TO UserA; 


-- Test is for the different users

EXECUTE AS USER = 'UserA';  
SELECT * FROM [dbo].[Orders];
SELECT OrderID,Course,Quantity FROM [dbo].[Orders];
REVERT;  
  
 
EXECUTE AS USER = 'Supervisor';  
SELECT * FROM [dbo].[Orders];
REVERT; 

-- Drop all of the artefacts

DROP USER Supervisor;
DROP USER UserA;

DROP TABLE [dbo].[Orders];




