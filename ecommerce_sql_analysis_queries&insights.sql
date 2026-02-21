-- Key metrics: Total Revenue, Orders, Customers, Products Sold
SELECT 
    COUNT(DISTINCT o.orderID) as total_orders,
    COUNT(DISTINCT o.user_id) as total_customers,
    SUM(pay.amount) as total_revenue,
    AVG(pay.amount) as avg_order_value,
    SUM(od.quantity) as total_items_sold,
    SUM(o.shipping_cost) as total_shipping_revenue,
    COUNT(DISTINCT CASE WHEN o.Status = 'cancelled' THEN o.orderID END) as cancelled_orders,
    CAST(COUNT(DISTINCT CASE WHEN o.Status = 'cancelled' THEN o.orderID END) * 100.0 / COUNT(DISTINCT o.orderID) AS DECIMAL(5,2)) as cancellation_rate_pct
FROM [Order] o
INNER JOIN Payment pay ON o.orderID = pay.Order_id
LEFT JOIN OrderDetails od ON o.orderID = od.order_id
WHERE pay.status = 'completed';
GO


-- avg shipping cost per city 
SELECT a.city, round(AVG(o.shipping_cost),2) AS avg_shipping_cost
FROM [Order] o JOIN Address a ON o.shipping_address_id = a.Address_id
GROUP BY a.city
ORDER BY avg_shipping_cost DESC;
GO

-- Top 3 Most Returned Products
SELECT TOP 3
    p.Name,
    SUM(r.returned_quantity) AS total_returned
FROM [Return] r
JOIN Product p ON r.product_id = p.productID
GROUP BY p.Name
ORDER BY total_returned DESC;
GO


-- Most Used Payment Method
SELECT 
    method,
    COUNT(paymentID) AS number_of_payments,
    SUM(amount) AS total_paid
FROM Payment
GROUP BY method
ORDER BY number_of_payments DESC;
GO


-- Monthly Sales Trend
SELECT 
    YEAR(o.order_created_at) AS order_year,
    MONTH(o.order_created_at) AS order_month,
    SUM(od.quantity * od.unit_price) AS monthly_sales
FROM [Order] o
JOIN OrderDetails od ON o.orderID = od.order_id
GROUP BY 
    YEAR(o.order_created_at),
    MONTH(o.order_created_at)
ORDER BY monthly_sales desc;
GO


-- Top 5 Customers by Revenue
WITH customer_sales AS (
    SELECT 
        u.UserID,
        u.first_name,
        u.last_name,
        SUM(od.quantity * od.unit_price) AS total_spent
    FROM [User] u
    JOIN [Order] o ON u.UserID = o.user_id
    JOIN OrderDetails od ON o.orderID = od.order_id
    GROUP BY u.UserID, u.first_name, u.last_name
)
SELECT *
FROM (
    SELECT *,
           RANK() OVER (ORDER BY total_spent DESC) AS customer_rank
    FROM customer_sales
) ranked
WHERE customer_rank <= 5;
GO


-- Product Sales vs Category Average
WITH product_sales AS (
    SELECT 
        p.productID,
        p.Name,
        p.category_ID,
        SUM(od.quantity * od.unit_price) AS product_revenue
    FROM Product p
    JOIN OrderDetails od ON p.productID = od.product_id
    GROUP BY p.productID, p.Name, p.category_ID
)
SELECT 
    Name,
    product_revenue,
    AVG(product_revenue) OVER (PARTITION BY category_ID) 
        AS category_avg_revenue
FROM product_sales;
GO



-- Running Monthly Revenue
WITH monthly_sales AS (
    SELECT 
        FORMAT(o.order_created_at, 'yyyy-MM') AS month,
        SUM(od.quantity * od.unit_price) AS total_sales
    FROM [Order] o
    JOIN OrderDetails od ON o.orderID = od.order_id
    GROUP BY FORMAT(o.order_created_at, 'yyyy-MM')
)
SELECT 
    month,
    total_sales,
    SUM(total_sales) OVER (ORDER BY month 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        AS running_total
FROM monthly_sales;
GO


-- Return Rate per Product
SELECT 
    p.Name,
    SUM(ISNULL(r.returned_quantity, 0)) AS total_returned,
    SUM(od.quantity) AS total_sold,
    CAST(SUM(ISNULL(r.returned_quantity, 0)) * 100.0 
         / SUM(od.quantity) AS DECIMAL(5,2)) AS return_rate_percentage
FROM Product p
JOIN OrderDetails od ON p.productID = od.product_id
LEFT JOIN [Return] r ON p.productID = r.product_id
GROUP BY p.Name
ORDER BY return_rate_percentage DESC;
GO



-- Average Rating per Product
SELECT 
    p.Name,
    round(AVG(r.rating),2) AS avg_rating,
    COUNT(r.ReviewID) AS total_reviews
FROM Product p
JOIN Review r ON p.productID = r.Product_id
GROUP BY p.Name
ORDER BY avg_rating DESC;
GO



-- Revenue per Category
SELECT 
    c.Name AS category_name,
    SUM(od.quantity * od.unit_price) AS total_revenue
FROM Category c
JOIN Product p ON c.categoryID = p.category_ID
JOIN OrderDetails od ON p.productID = od.product_id
GROUP BY c.Name
ORDER BY total_revenue DESC;
GO



-- Customers With No Orders
SELECT 
    u.UserID,
    u.first_name,
    u.last_name
FROM [User] u
LEFT JOIN [Order] o ON u.UserID = o.user_id
WHERE o.orderID IS NULL;
GO



-- Products never ordered
SELECT productID, [Name], price, Brand, discount_percentage
FROM Product
WHERE productID NOT IN
      (SELECT DISTINCT product_id FROM OrderDetails);
      GO



-- top 10 Best-selling products
SELECT top 10 p.Name,
       SUM(od.quantity) AS TotalSold
FROM OrderDetails od
JOIN Product p ON od.product_id = p.productID
GROUP BY p.Name
ORDER BY TotalSold DESC;
GO



-- cities by number of orders
SELECT a.city,
       COUNT(o.orderID) AS OrdersCount
FROM Address a
JOIN [Order] o ON a.Address_id = o.shipping_address_id
GROUP BY a.city
ORDER BY OrdersCount DESC;
GO



-- Daily Sales Pattern - Which days of the week are most profitable?
SELECT 
    DATENAME(WEEKDAY, o.order_created_at) as day_of_week,
    DATEPART(WEEKDAY, o.order_created_at) as day_number,
    COUNT(DISTINCT o.orderID) as orders,
    SUM(pay.amount) as revenue,
    AVG(pay.amount) as avg_order_value
FROM [Order] o
INNER JOIN Payment pay ON o.orderID = pay.Order_id
WHERE pay.status = 'completed'
GROUP BY DATENAME(WEEKDAY, o.order_created_at), DATEPART(WEEKDAY, o.order_created_at)
ORDER BY day_number;
GO
