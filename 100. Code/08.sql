-- Lab - Building a dimension table
-- Lets build a view for the customers

CREATE VIEW Customer_view 
AS
  SELECT ct.[CustomerID],ct.[StoreID],st.[BusinessEntityID],st.[Name]  as StoreName
  FROM [Sales].[Customer] as ct
  LEFT JOIN [Sales].[Store] as st 
  ON ct.[StoreID]=st.[BusinessEntityID]
  WHERE  st.[BusinessEntityID] IS NOT NULL

-- Lets create a customer dimension table

SELECT [CustomerID],[StoreID],[BusinessEntityID],StoreName
INTO DimCustomer
FROM Customer_view 


-- Lets build a view for the products

CREATE VIEW Product_view 
AS
SELECT prod.[ProductID],prod.[Name] as ProductName,prod.[SafetyStockLevel],model.[ProductModelID],model.[Name] as ProductModelName,category.[ProductSubcategoryID],category.[Name] AS ProductSubCategoryName
FROM [Production].[Product] prod
LEFT JOIN [Production].[ProductModel] model ON prod.[ProductModelID] = model.[ProductModelID]
LEFT JOIN [Production].[ProductSubcategory] category ON prod.[ProductSubcategoryID]=category.[ProductSubcategoryID]
WHERE prod.[ProductModelID] IS NOT NULL

-- Lets create a product dimension table

SELECT [ProductID],[ProductModelID],[ProductSubcategoryID],ProductName,[SafetyStockLevel],ProductModelName,ProductSubCategoryName
INTO DimProduct
FROM Product_view 

-- If you want to drop the views and the tables

DROP VIEW Customer_view 

DROP TABLE DimCustomer

DROP VIEW Product_view 

DROP TABLE DimProduct

