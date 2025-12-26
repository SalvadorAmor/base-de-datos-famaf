# Obtener los usuarios que han gastado más en reservas

USE airbnb_like_db;

SELECT users.*, SUM(bookings.total_price) as total
FROM users
    INNER JOIN bookings
ON users.id = bookings.user_id
GROUP BY users.id
ORDER BY total  DESC;

# Obtener las 10 propiedades con el mayor ingreso total por reservas

SELECT properties.*, SUM(total_price) as total
FROM properties
    INNER JOIN bookings ON properties.id = bookings.property_id
GROUP BY properties.id
ORDER BY total DESC
LIMIT 10;

# Crear un trigger para registrar automáticamente reseñas negativas en la tabla de
# mensajes. Es decir, el owner recibe un mensaje al obtener un review menor o igual a 2.
DROP TRIGGER IF EXISTS bad_reviews_notification;
DELIMITER //
CREATE TRIGGER bad_reviews_notification AFTER INSERT ON reviews
    FOR EACH ROW
    BEGIN
        IF NEW.rating <= 2 THEN
            INSERT INTO messages(sender_id, receiver_id, property_id, content)
            SELECT NEW.user_id,properties.owner_id,NEW.property_id,'Ha recibido una reseña negativa'
            FROM properties WHERE properties.id = NEW.property_id;
        END IF;
    END //
DELIMITER ;

# Crear un procedimiento llamado process_payment que:
# Reciba los siguientes parámetros:
# - input_booking_id (INT): El ID de la reserva.
# - input_user_id (INT): El ID del usuario que realiza el pago.
# - input_amount (NUMERIC): El monto del pago.
# - input_payment_method (VARCHAR): El metodo de pago utilizado (por ejemplo,
# "credit_card", "paypal").
# Requisitos: verificar si la reserva asociada existe y está en estado confirmed. Insertar
# un nuevo registro en la tabla payments. Actualizar el estado de la reserva a paid.
# No es necesario manejar errores ni transacciones en este procedimiento.
DROP PROCEDURE IF EXISTS process_payment;
DELIMITER //
CREATE PROCEDURE process_payment (
    IN input_booking_id INT,
    IN input_user_id INT,
    IN input_amount NUMERIC,
    IN input_payment_method VARCHAR(20)
    )
BEGIN
    IF EXISTS(SELECT * FROM bookings WHERE id = input_booking_id AND status = 'confirmed') THEN
     BEGIN
         INSERT INTO payments(booking_id, user_id, amount, payment_method)
         VALUES (input_booking_id,input_user_id,input_amount,input_payment_method);
         UPDATE bookings SET status = 'paid' WHERE id = input_booking_id;
     END;
    END IF;
END //
DELIMITER ;

CALL process_payment(1302,1747,10,'paypal')


