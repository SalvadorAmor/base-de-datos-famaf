-- Crear un campo nuevo `total_medals` en la tabla `person` que almacena la
-- cantidad de medallas ganadas por cada persona. Por defecto, con valor 0.

USE olympics;

ALTER TABLE person
    ADD COLUMN total_medals INT NOT NULL DEFAULT '0';

# Actualizar la columna `total_medals` de cada persona con el recuento real de
# medallas que ganó. Por ejemplo, para Michael Fred Phelps II, luego de la
# actualización debería tener como valor de `total_medals` igual a 28.
WITH medals_per_person AS (
    SELECT COUNT(competitor_event.medal_id) as medals, games_competitor.person_id
    FROM games_competitor
    INNER JOIN competitor_event ON games_competitor.id = competitor_event.competitor_id
    GROUP BY person_id
)
UPDATE person
INNER JOIN medals_per_person
SET total_medals = medals_per_person.medals
WHERE person.id = medals_per_person.person_id;

# Devolver todos los medallistas olímpicos de Argentina, es decir, los que hayan
# logrado alguna medalla de oro, plata, o bronce, enumerando la cantidad por tipo de
# medalla. Por ejemplo, la query debería retornar casos como el siguiente:
# (Juan Martín del Potro, Bronze, 1), (Juan Martín del Potro, Silver,1)
SELECT person.full_name, medal.medal_name, COUNT(medal_id)
FROM person
INNER JOIN games_competitor ON person.id = games_competitor.person_id
INNER JOIN competitor_event ON games_competitor.id = competitor_event.competitor_id
INNER JOIN medal ON competitor_event.medal_id = medal.id AND medal_name != 'NA'
GROUP BY full_name, medal_name;

# Listar el total de medallas ganadas por los deportistas argentinos en cada deporte

SELECT count(competitor_event.medal_id) as medals, sport_name
FROM person
INNER JOIN person_region ON person.id = person_region.person_id
INNER JOIN noc_region ON person_region.region_id = noc_region.id
INNER JOIN games_competitor ON person.id = games_competitor.person_id
INNER JOIN competitor_event ON games_competitor.id = competitor_event.competitor_id
INNER JOIN medal ON competitor_event.medal_id = medal.id
INNER JOIN event ON competitor_event.event_id = event.id
INNER JOIN sport ON event.sport_id = sport.id
WHERE medal_name != 'NA' AND noc_region.noc = 'ARG'
GROUP BY sport_name;

# Listar el número total de medallas de oro, plata y bronce
# ganadas por cada país (país representado en la tabla noc_region), agruparlas los resultados por pais.

SELECT nr.region_name AS pais,
       SUM(CASE WHEN m.medal_name = 'Gold'   THEN 1 ELSE 0 END) AS oro,
       SUM(CASE WHEN m.medal_name = 'Silver' THEN 1 ELSE 0 END) AS plata,
       SUM(CASE WHEN m.medal_name = 'Bronze' THEN 1 ELSE 0 END) AS bronce
FROM noc_region nr
JOIN person_region pr ON pr.region_id = nr.id
JOIN person p          ON p.id = pr.person_id
JOIN games_competitor gc ON gc.person_id = p.id
JOIN competitor_event ce ON ce.competitor_id = gc.id
JOIN medal m             ON m.id = ce.medal_id
WHERE ce.medal_id IS NOT NULL AND m.medal_name != 'NA'
GROUP BY nr.region_name
ORDER BY oro DESC, plata DESC, bronce DESC;
