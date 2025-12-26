USE airbnb_like_db;

-- Listar las 7 propiedades con la mayor cantidad de reviews en el año 2024.
-- LEFT JOIN? nos traemos propiedades con 0 reviews

SELECT properties.*
FROM properties
INNER JOIN reviews ON properties.id = reviews.property_id
WHERE YEAR(reviews.created_at) = 2024
GROUP BY properties.id
ORDER BY COUNT(reviews.id) DESC
LIMIT 7;

# Obtener los ingresos por reservas de cada propiedad.
# Esta consulta debe calcular los ingresos totales generados por cada propiedad.
# Ayuda: hay un campo `price_per_night` en la tabla de `properties` donde los
# ingresos totales se computan sumando la cantidad de noches reservadas para cada
# reserva multiplicado por el precio por noche.

SELECT SUM(DATEDIFF(bookings.check_out, bookings.check_in)*properties.price_per_night) as ingresos, properties.*
FROM properties
INNER JOIN bookings ON properties.id = bookings.property_id
GROUP BY properties.id;

-- ----------------------------------------

DELIMITER //
CREATE FUNCTION total_income (
	prop_id INT
) RETURNS DECIMAL(10,2)
READS SQL DATA
BEGIN
	DECLARE night_price DECIMAL(10,2) DEFAULT 0;
	DECLARE total DECIMAL(10,2) DEFAULT 0;

    SELECT price_per_night INTO night_price
    FROM properties WHERE id = prop_id;

    SELECT
		sum(DATEDIFF(b.check_out, b.check_in))*night_price
    INTO total
    FROM bookings AS b
    WHERE b.property_id = prop_id
    GROUP BY b.property_id;

    RETURN total;
END //
DELIMITER ;

SELECT p.name, total_income(p.id) as total_income
FROM properties as p;

-- -----------------------------------------------

# Listar los principales usuarios según los pagos totales.
# Esta consulta calcula los pagos totales realizados por cada usuario y enumera los
# principales 10 usuarios según la suma de sus pagos.

SELECT SUM(payments.amount) AS total_payment, `users`.*
FROM `users`
INNER JOIN payments ON `users`.id = payments.user_id
GROUP BY `users`.id
ORDER BY total_payment DESC
LIMIT 10;

# Crear un trigger notify_host_after_booking que notifica al anfitrión sobre una nueva
# reserva. Es decir, cuando se realiza una reserva, notifique al anfitrión de la propiedad
# mediante un mensaje.
DELIMITER //
CREATE TRIGGER notify_host_after_booking AFTER INSERT ON bookings
    FOR EACH ROW BEGIN
        INSERT INTO messages(sender_id, receiver_id, property_id, content)
        SELECT NEW.user_id, properties.owner_id, NEW.property_id, 'Se creo una reserva'
        FROM properties
        WHERE properties.id = NEW.property_id;
    END//
DELIMITER ;

# Crear un procedimiento add_new_booking para agregar una nueva reserva.
# Este procedimiento agrega una nueva reserva para un usuario, según el ID de la
# propiedad, el ID del usuario y las fechas de entrada y salida. Verifica si la propiedad
# está disponible durante las fechas especificadas antes de insertar la reserva.
DELIMITER //
CREATE PROCEDURE add_new_booking (
    IN user_id INT,
    IN property_id INT,
    IN check_in DATE,
    IN check_out DATE
)
BEGIN
    DECLARE booking_price INT;
    DECLARE dates_diff INT;

    IF NOT EXISTS(SELECT *
                  FROM bookings b
                  WHERE b.property_id = property_id
                  AND ((check_in BETWEEN  b.check_in AND b.check_out)
                  OR check_out BETWEEN b.check_in AND b.check_out)) THEN
        BEGIN
            SELECT properties.price_per_night INTO booking_price
            FROM properties WHERE property_id = properties.id;

            SET dates_diff = DATEDIFF(check_out, check_in);

            INSERT INTO bookings(property_id, user_id, check_in, check_out, total_price)
            VALUES(add_new_booking.property_id,
                   add_new_booking.user_id,
                   add_new_booking.check_in,
                   add_new_booking.check_out,
                   dates_diff*booking_price);
        END;
    END IF;
END //

# Crear el rol `admin` y asignarle permisos de creación sobre la tabla `properties` y
# permiso de actualización sobre la columna `status` de la tabla
# `property_availability`

CREATE ROLE `admin`;
GRANT INSERT ON airbnb_like_db.properties TO `admin`;
GRANT UPDATE (`status`) ON airbnb_like_db.property_availability TO `admin`;