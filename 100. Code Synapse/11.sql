-- Lab - Creating Round-robin Tables

-- First let's ensure we have the tables defined in the SQL pool
-- We will use Azure Data Factory to transfer the tables

CREATE TABLE [dbo].[SalesFact](
	[ProductID] [int] NOT NULL,
	[SalesOrderID] [int] NOT NULL,
	[CustomerID] [int] NOT NULL,
	[OrderQty] [smallint] NOT NULL,
	[UnitPrice] [money] NOT NULL,
	[OrderDate] [datetime] NULL,
	[TaxAmt] [money] NULL
)


-- To see the distribution on the table
DBCC PDW_SHOWSPACEUSED('[dbo].[SalesFact]')

-- If you execute the below query
SELECT [CustomerID], COUNT([CustomerID]) as COUNT FROM [dbo].[SalesFact]
GROUP BY [CustomerID]
ORDER BY [CustomerID]
