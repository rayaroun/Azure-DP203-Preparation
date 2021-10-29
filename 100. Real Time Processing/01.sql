-- Lab - Azure Stream Analytics - Defining the job

CREATE TABLE [dbo].[Orders]
(
	[OrderID] varchar(10),
	[Quantity] int,
	[UnitPrice] decimal(5,2),
	[DiscountCategory] varchar(10)
)


SELECT
    OrderID,Quantity,UnitPrice,DiscountCategory
INTO
    OrderSynapse
FROM
    programinput