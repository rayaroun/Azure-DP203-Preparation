-- Lab - Example when using the right distributions for your tables

DROP TABLE [dbo].[SalesFact]


CREATE TABLE [dbo].[SalesFact](
	[ProductID] [int] NOT NULL,
	[SalesOrderID] [int] NOT NULL,
	[CustomerID] [int] NOT NULL,
	[OrderQty] [smallint] NOT NULL,
	[UnitPrice] [money] NOT NULL,
	[OrderDate] [datetime] NULL,
	[TaxAmt] [money] NULL
)
WITH  
(   
    DISTRIBUTION = HASH (ProductID)
)

-- Create the dimension table with round-robin distribution

DROP TABLE [dbo].[DimProduct]

CREATE TABLE [dbo].[DimProduct](
	[ProductID] [int] NOT NULL,
	[ProductModelID] [int] NOT NULL,
	[ProductSubcategoryID] [int] NOT NULL,
	[ProductName] varchar(50) NOT NULL,
	[SafetyStockLevel] [smallint] NOT NULL,
	[ProductModelName] varchar(50) NULL,
	[ProductSubCategoryName] varchar(50) NULL
)

-- Perform a JOIN on the fact and dimension table

SELECT ft.[ProductID],pd.[ProductName]
FROM [dbo].[SalesFact] ft JOIN [dbo].[DimProduct] pd
ON  ft.[ProductID]=pd.[ProductID]


-- Create the dimension table with replicated distribution

DROP TABLE [dbo].[DimProduct]

CREATE TABLE [dbo].[DimProduct](
	[ProductID] [int] NOT NULL,
	[ProductModelID] [int] NOT NULL,
	[ProductSubcategoryID] [int] NOT NULL,
	[ProductName] varchar(50) NOT NULL,
	[SafetyStockLevel] [smallint] NOT NULL,
	[ProductModelName] varchar(50) NULL,
	[ProductSubCategoryName] varchar(50) NULL
)
WITH  
(   
    DISTRIBUTION = REPLICATE
)





