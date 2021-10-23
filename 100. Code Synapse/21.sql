-- Creating a heap table

CREATE TABLE [dbo].[SalesFact_staging](
	[ProductID] [int] NOT NULL,
	[SalesOrderID] [int] NOT NULL,
	[CustomerID] [int] NOT NULL,
	[OrderQty] [smallint] NOT NULL,
	[UnitPrice] [money] NOT NULL,
	[OrderDate] [datetime] NULL,
	[TaxAmt] [money] NULL
)
WITH(HEAP,
DISTRIBUTION = ROUND_ROBIN
)

CREATE INDEX ProductIDIndex ON [dbo].[SalesFact_staging] (ProductID)
