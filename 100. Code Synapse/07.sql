-- Lab - Building a Fact Table
-- Lets first create a view

  CREATE VIEW [Sales_Fact_View]
  AS
  SELECT dt.[ProductID],dt.[SalesOrderID],dt.[OrderQty],dt.[UnitPrice],hd.[OrderDate],hd.[CustomerID],hd.[TaxAmt]
  FROM [Sales].[SalesOrderDetail] dt
  LEFT JOIN [Sales].[SalesOrderHeader] hd
  ON dt.[SalesOrderID]=hd.[SalesOrderID]

-- Then we will create the Sales Fact table from the view
  
SELECT [ProductID],[SalesOrderID],[CustomerID],[OrderQty],[UnitPrice],[OrderDate],[TaxAmt]
INTO SalesFact
FROM Sales_Fact_View
