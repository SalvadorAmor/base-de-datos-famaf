# Listar el nombre de la ciudad y el nombre del país de todas las ciudades
# que pertenezcan a países con una población menor a 10000 habitantes.

SELECT city.Name, country.Name
FROM city
INNER JOIN country ON CountryCode = Code
WHERE country.Population < 10000;

SELECT city.Name, c.Name
FROM city
JOIN (
    SELECT Code, Name
    FROM country
    WHERE Population < 10000
) AS c
ON city.CountryCode = c.Code;

# Listar todas aquellas ciudades cuya población sea mayor que la población promedio entre todas las ciudades.

SELECT city.Name FROM city WHERE Population > ALL (SELECT avg(city.Population) FROM city);

# Listar todas aquellas ciudades no asiáticas cuya población sea igual o mayor a la población total de algún país de Asia.
WITH asia_countries AS
    (SELECT country.Code, country.Population FROM country WHERE Continent = 'Asia')
SELECT city.Name FROM city WHERE CountryCode NOT IN (SELECT Code FROM asia_countries)
                             AND Population >= SOME (SELECT Code FROM asia_countries);

# Listar (sin duplicados) aquellas regiones que tengan países con una superficie menor a 1000 km2
# y exista (en el país) al menos una ciudad con más de 100000 habitantes.
# (Hint: Esto puede resolverse con o sin una subquery, intenten encontrar ambas respuestas).

SELECT country.Region
FROM country
WHERE country.SurfaceArea < 1000
AND EXISTS(SELECT * FROM city WHERE country.Code = city.CountryCode AND city.Population > 100000);

SELECT DISTINCT country.Region
FROM country
INNER JOIN city ON CountryCode = Code
WHERE city.Population > 100000 AND SurfaceArea < 1000;

# La primera forma no permite duplicados, mientras que la segunda forma si no tuviera el DISTINCT, si lo permitiria.

# Listar el nombre de cada país con la cantidad de habitantes de su ciudad más poblada.
# (Hint: Hay dos maneras de llegar al mismo resultado. Usando consultas escalares o usando agrupaciones, encontrar ambas).

SELECT country.Name, (SELECT max(city.Population)
                      FROM city
                      WHERE CountryCode = Code) AS max_population
FROM country;

# Este no es mejor

SELECT country.Name,max(city.Population)
FROM city
INNER JOIN country ON CountryCode = Code
GROUP BY country.Name;

# Listar aquellos países y sus lenguajes no oficiales cuyo porcentaje de hablantes
# sea mayor al promedio de hablantes de los lenguajes oficiales.



SELECT country.Name, countrylanguage.Language
FROM country
INNER JOIN countrylanguage ON Code = CountryCode
WHERE IsOfficial = 'F' AND Percentage > (SELECT avg(Percentage)
                                         FROM countrylanguage
                                         WHERE IsOfficial = 'T' AND Code = CountryCode);