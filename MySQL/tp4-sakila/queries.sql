USE sakila;

-- Punto 1
DROP TABLE IF EXISTS directors;

CREATE TABLE directors(
    director_id int NOT NULL AUTO_INCREMENT,
    first_name char(20) NOT NULL DEFAULT '',
    last_name char(20) NOT NULL DEFAULT '',
    movies_amount int NOT NULL DEFAULT 0,
    PRIMARY KEY (director_id)
);

-- Punto 2

INSERT INTO directors(first_name, last_name, movies_amount)
SELECT actor.first_name, actor.last_name, count(film_actor.actor_id) as movies_amount
FROM actor
INNER JOIN film_actor ON actor.actor_id = film_actor.actor_id
GROUP BY film_actor.actor_id
ORDER BY movies_amount DESC
LIMIT 5;

-- ESTA ES LA TECA (Menos eficiente)

INSERT INTO directors (first_name, last_name, movies_amount)
SELECT a.first_name,
       a.last_name,
       (SELECT COUNT(*) FROM film_actor fa WHERE fa.actor_id = a.actor_id) AS movies_amount
FROM actor a
ORDER BY movies_amount DESC
LIMIT 5;

-- Punto 3

ALTER TABLE customer
ADD COLUMN premium_customer enum('T', 'F') NOT NULL DEFAULT 'F';

-- Punto 4

WITH best_customers(customer_id, money_spent) AS (
    SELECT payment.customer_id, sum(payment.amount) AS money_spent
    FROM payment
    GROUP BY payment.customer_id
    ORDER BY money_spent DESC
    LIMIT 10
)
UPDATE customer
SET premium_customer = 'T'
WHERE customer.customer_id IN (SELECT customer_id FROM best_customers);

-- Punto 5

SELECT film.rating, count(film.film_id) as amount FROM film
GROUP BY film.rating
ORDER BY amount DESC;

-- Punto 6

(SELECT payment_date FROM payment ORDER BY payment_date DESC LIMIT 1)
UNION
(SELECT payment_date FROM payment ORDER BY payment_date ASC LIMIT 1);

-- Punto 7

SELECT MONTH(payment.payment_date) as month, AVG(payment.amount) FROM payment GROUP BY month;

-- Punto 8

SELECT address.district,
       COUNT(rental.rental_id) AS total_alquileres
FROM rental
INNER JOIN customer ON rental.customer_id = customer.customer_id
INNER JOIN address ON customer.address_id = address.address_id
GROUP BY address.district
ORDER BY total_alquileres DESC
LIMIT 10;

-- Punto 9

ALTER TABLE inventory ADD COLUMN stock int NOT NULL DEFAULT '5'

-- Cree un trigger `update_stock` que, cada vez que se agregue un nuevo registro a la tabla rental, haga un update en
-- la tabla `inventory` restando una copia al stock de la película rentada (Hint: revisar que el rental no tiene
-- información directa sobre la tienda, sino sobre el cliente, que está asociado a una tienda en particular).

CREATE TRIGGER update_stock AFTER INSERT ON rental
    FOR EACH ROW BEGIN
        UPDATE inventory INNER JOIN customer
        ON inventory.store_id = customer.store_id
        SET stock = stock - 1 WHERE inventory.inventory_id = NEW.inventory_id
                                and customer.customer_id = NEW.customer_id
                                and inventory.stock > 0;
    END;
--

CREATE TRIGGER update_stock AFTER INSERT ON rental
    FOR EACH ROW BEGIN
    UPDATE inventory
    SET stock = stock - 1
    WHERE inventory_id = NEW.inventory_id
      AND stock > 0;
    END;

-- Cree una tabla `fines` que tenga dos campos: `rental_id` y `amount`.
-- El primero es una clave foránea a la tabla rental y el segundo es un valor numérico con dos decimales.

CREATE TABLE fines (
    fine_id INT NOT NULL AUTO_INCREMENT,
    rental_id INT NOT NULL DEFAULT '0',
    amount decimal (10,2) NOT NULL DEFAULT '0.0',
    PRIMARY KEY (fine_id),
    FOREIGN KEY (rental_id) REFERENCES rental (rental_id)
);

-- Cree un procedimiento `check_date_and_fine` que revise la tabla `rental` y cree un registro en la tabla `fines`
-- por cada `rental` cuya devolución (return_date) haya tardado más de 3 días (comparación con rental_date).
-- El valor de la multa será el número de días de retraso multiplicado por 1.5.

DELIMITER //
CREATE PROCEDURE check_date_and_fine()
    BEGIN
        INSERT INTO fines (rental_id, amount)
        SELECT rental.rental_id,(DATEDIFF(rental.return_date,rental.rental_date) - 3)*1.5 as amount
        FROM rental WHERE DATEDIFF(rental.return_date,rental.rental_date) > 3
        ON DUPLICATE KEY UPDATE
            amount = VALUES(amount);
    END//
DELIMITER ;

CALL check_date_and_fine();

create role employee;
grant insert, update, delete ON sakila.rental to employee;

-- crear un rol `administrator` que tenga todos los privilegios sobre la BD `sakila`.
revoke delete on sakila.rental from employee;
create role administrator;
grant ALL privileges on sakila.* to administrator;

-- Crear dos roles de empleado. A uno asignarle los permisos de `employee` y al otro de `administrator`.
create role empleado1, empleado2;
grant administrator to empleado1;
grant employee to empleado2;
