-- Lab - CASE statement

CREATE TABLE [ProductDetails]
(
   [productid] int,
   [productname] varchar(20),
   [productstatus] varchar(1),
   [quantity] int
)


SELECT [productid],[productname],
status = CASE [productstatus]
WHEN 'W' THEN 'Warehouse'
WHEN 'S' THEN 'Store'
WHEN 'T' THEN 'Transit'
END
FROM [ProductDetails]