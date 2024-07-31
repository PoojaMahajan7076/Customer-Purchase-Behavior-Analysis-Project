-- Setup a Database & Data Ingestion

Create database Project;

-- Data Transformation
-- Normalization of the data & Creating relationships between tables.

use Project;

CREATE TABLE Customer (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(255),
    Country VARCHAR(255)
);

INSERT INTO Customer (CustomerID, CustomerName, Country)
WITH cte AS (
    SELECT customername, country, purchasedate 
    FROM customer_purchase_data
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY purchasedate) + 500 AS CustomerID, 
    customername AS CustomerName, 
    country AS Country 
FROM cte;


CREATE TABLE Product (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    ProductCategory VARCHAR(255)
);

INSERT INTO Product (ProductID, ProductName, ProductCategory)
WITH cte1 AS (
    SELECT productname, productcategory, purchasedate 
    FROM customer_purchase_data
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY purchasedate) + 3000 AS ProductID, 
    productname AS ProductName, 
    productcategory AS ProductCategory 
FROM cte1;

-- Create temporary table for ProductID mapping
CREATE TEMPORARY TABLE Product_IDMapping AS
SELECT 
    productname, 
    MIN(ProductID) AS ProductID
FROM Product
GROUP BY productname;

-- Create temporary table for CustomerID mapping
CREATE TEMPORARY TABLE Customer_IDMapping AS
SELECT 
    customername, 
    MIN(CustomerID) AS CustomerID
FROM Customer
GROUP BY customername;

select * from customer_purchase_data;
CREATE TABLE Purchase (
    TransactionID INT PRIMARY KEY,
    CustomerID INT,
    ProductID INT,
    PurchaseQuantity INT,
    PurchasePrice DECIMAL(10, 2),
    PurchaseDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
);

INSERT INTO Purchase (TransactionID, CustomerID, ProductID, PurchaseQuantity, PurchasePrice, PurchaseDate)
WITH cte2 AS (
    SELECT 
        transactionid,
        customername,
        productname,
        purchasequantity,
        purchaseprice,
        purchasedate
    FROM customer_purchase_data
    WHERE transactionid NOT IN (SELECT TransactionID FROM Purchase)
)
SELECT 
    cte2.transactionid AS TransactionID,
    cm.CustomerID,
    pm.ProductID,
    cte2.purchasequantity AS PurchaseQuantity,
    cte2.purchaseprice AS PurchasePrice,
    STR_TO_DATE(cte2.purchasedate, '%m/%d/%Y') AS PurchaseDate
FROM cte2
JOIN Customer_IDMapping cm ON cm.customername = cte2.customername
JOIN Product_IDMapping pm ON pm.productname = cte2.productname;



select * from customer;

select * from product;

select * from purchase;

-- Handling missing values.
select * from customer where customername is null or customerid is null or country is null;
select * from product where productname is null or productid is null or productcategory is null;
select * from purchase where transactionid is null or customerid is null or productid is null or 
purchasequantity is null or purchaseprice is null or purchasedate is null;



    