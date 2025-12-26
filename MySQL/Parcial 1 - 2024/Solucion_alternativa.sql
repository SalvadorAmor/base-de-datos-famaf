# Intento 1

SET GLOBAL log_bin_trust_function_creators = 1;
DELIMITER //
CREATE FUNCTION max_sales_per_year (year YEAR) RETURNS INT
    BEGIN
        DECLARE max_sales INT;
        WITH best AS (
            SELECT Orders.EmployeeID, YEAR(Orders.OrderDate) as Year, COUNT(OrderID) as TotalSales
            FROM Orders
            GROUP BY EmployeeID, YEAR(Orders.OrderDate)
         )
        SELECT MAX(TotalSales)
        INTO max_sales
        FROM best
        WHERE best.Year = year;
        RETURN max_sales;
    END//
DELIMITER ;

DELIMITER //

CREATE FUNCTION orderID_count (empID INT,year YEAR) RETURNS INT
    BEGIN
        DECLARE orderCounts INT;
        WITH best AS (
            SELECT Orders.EmployeeID, YEAR(Orders.OrderDate) as Year, COUNT(OrderID) as TotalSales
            FROM Orders
            GROUP BY EmployeeID, YEAR(Orders.OrderDate)
         )
        SELECT TotalSales
        INTO orderCounts
        FROM best
        WHERE best.Year = year AND best.EmployeeID = empID;
        RETURN orderCounts;
    END //
    DELIMITER ;

CREATE VIEW best_employees AS
SELECT DISTINCT Employees.FirstName, Employees.LastName, YEAR(Orders.OrderDate) as Year, orderID_count(Orders.EmployeeID, YEAR(Orders.OrderDate))
FROM Orders
INNER JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID
WHERE orderID_count(Orders.EmployeeID, YEAR(Orders.OrderDate)) = max_sales_per_year(YEAR(Orders.OrderDate))
ORDER BY Year ASC;
