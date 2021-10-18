-- This is just a sample query that can be used to join both the Dimension and the Fact table

SELECT dimc.StoreName,dimp.ProductName,dimp.ProductSubCategoryName,SUM(ft.OrderQty) AS Quantity
FROM SalesFact ft
INNER JOIN DimCustomer dimc ON ft.CustomerID=dimc.CustomerID
INNER JOIN DimProduct dimp ON ft.ProductID=dimp.ProductID
GROUP BY dimc.StoreName,dimp.ProductName,dimp.ProductSubCategoryName
