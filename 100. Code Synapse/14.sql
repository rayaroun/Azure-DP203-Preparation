-- Lab - Windowing Functions

SELECT
ROW_NUMBER() OVER(PARTITION BY [ProductID] ORDER BY [OrderQty]) AS "Row Number",
[ProductID],[CustomerID],[OrderQty],[UnitPrice]
FROM [dbo].[SalesFact]
ORDER BY [ProductID]

SELECT
ROW_NUMBER() OVER(PARTITION BY [ProductID] ORDER BY [OrderQty]) AS "Row Number",
[ProductID],[CustomerID],[OrderQty],
SUM([OrderQty]) OVER(PARTITION BY [ProductID]) AS TotalOrderQty,
[UnitPrice]
FROM [dbo].[SalesFact]
ORDER BY [ProductID]
