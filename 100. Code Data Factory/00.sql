-- Lab - Surrogate Keys - Dimension tables

DROP TABLE [dbo].[DimProduct]

CREATE TABLE [dbo].[DimProduct](
	[ProductSK] [int] NOT NULL,
	[ProductID] [int] NOT NULL,
	[ProductModelID] [int] NOT NULL,
	[ProductSubcategoryID] [int] NOT NULL,
	[ProductName] varchar(50) NOT NULL,
	[SafetyStockLevel] [smallint] NOT NULL,
	[ProductModelName] varchar(50) NULL,
	[ProductSubCategoryName] varchar(50) NULL
)