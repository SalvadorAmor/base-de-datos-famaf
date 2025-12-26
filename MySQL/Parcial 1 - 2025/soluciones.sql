USE northwind;


# Listar los 5 clientes que más ingresos han generado a lo largo del tiempo.

SELECT Customers.*
FROM Customers
INNER JOIN Orders ON Customers.CustomerID = Orders.CustomerID
INNER JOIN `Order Details` od ON Orders.OrderID = od.OrderID
GROUP BY Customers.CustomerID
ORDER BY
    SUM((od.UnitPrice)*od.Quantity*(1-od.Discount)) DESC
LIMIT 5;

# Listar cada producto con sus ventas totales, agrupados por categoría.

SELECT Categories.CategoryName, Products.ProductName, COUNT(od.ProductID)*od.Quantity as TotalSales
FROM Categories
INNER JOIN Products ON Categories.CategoryID = Products.CategoryID
INNER JOIN `Order Details` od ON Products.ProductID = od.ProductID
GROUP BY CategoryName, ProductName, Quantity;

# Calcular el total de ventas para cada categoría.

SELECT Categories.CategoryName, COUNT(od.OrderID) as TotalSales
FROM Categories
INNER JOIN Products ON Categories.CategoryID = Products.CategoryID
INNER JOIN `Order Details` od ON Products.ProductID = od.ProductID
GROUP BY CategoryName;

# Crear una vista que liste los empleados con más ventas por cada año, mostrando
# empleado, año y total de ventas. Ordenar el resultado por año ascendente.

# Perdon por el orden se logro 5 minutos antes de terminar :(

CREATE VIEW best_employees AS
WITH max_year AS (
    SELECT Orders.EmployeeID, YEAR(Orders.OrderDate) as Year_max, COUNT(OrderID) as TotalSales
    FROM Orders
    GROUP BY EmployeeID, YEAR(Orders.OrderDate)
)
SELECT employee_sells.EmployeeID, Year, max_sale
FROM (
    SELECT Year_max as y_max, MAX(TotalSales) as max_sale
    FROM max_year
    GROUP BY Year_max
) AS year_max
INNER JOIN (
    SELECT EmployeeID, Year_max as Year, TotalSales
    FROM max_year
) AS employee_sells ON TotalSales = max_sale;

# Crear un trigger que se ejecute después de insertar un nuevo registro en la tabla
# Order Details. Este trigger debe actualizar la tabla Products para disminuir la
# cantidad en stock (UnitsInStock) del producto correspondiente, restando la
# cantidad (Quantity) que se acaba de insertar en el detalle del pedido

DELIMITER //
CREATE TRIGGER less_stock AFTER INSERT on `Order Details`
    FOR EACH ROW BEGIN
        UPDATE Products
        SET UnitsInStock = UnitsInStock - NEW.Quantity
        WHERE ProductID = NEW.ProductID;
    END //
DELIMITER ;

# Crear un rol llamado admin y otorgarle los siguientes permisos:
# ● crear registros en la tabla Customers.
# ● actualizar solamente la columna Phone de Customers.

CREATE ROLE `admin_T`;
GRANT INSERT ON northwind.Customers TO `admin_T`;
GRANT UPDATE (`Phone`) ON northwind.Customers TO `admin_T`;
