
SELECT city.Name, country.Name, country.Region, country.GovernmentForm
FROM city INNER JOIN country ON country.Code = city.CountryCode ORDER BY city.Population DESC LIMIT 10;


SELECT country.Name, city.Name 
FROM country LEFT JOIN city ON country.Capital = city.ID ORDER BY country.Population ASC LIMIT 10;

SELECT country.Name, country.Continent, countrylanguage.Language 
FROM country INNER JOIN countrylanguage ON countrylanguage.CountryCode = country.Code;

SELECT country.Name , city.Name FROM country INNER JOIN city ON country.Capital = city.ID 
ORDER BY country.SurfaceArea DESC LIMIT 20;

SELECT city.Name, countrylanguage.Language, countrylanguage.Percentage
FROM city INNER JOIN countrylanguage 
ON city.CountryCode = countrylanguage.CountryCode AND countrylanguage.IsOfficial = 'T' 
ORDER BY city.Population DESC;

(SELECT Name, Population FROM country ORDER BY Population DESC LIMIT 10) 
UNION 
(SELECT Name, Population FROM country WHERE Population >= 100 ORDER BY Population ASC LIMIT 10);

SELECT country.Name, countrylanguage.Language FROM country INNER JOIN countrylanguage ON 
(countrylanguage.Language = 'English' OR countrylanguage.Language = 'French') 
AND countrylanguage.CountryCode = country.Code AND countrylanguage.IsOfficial = 'T';

SELECT country.Name FROM country INNER JOIN countrylanguage ON countrylanguage.Language = 'English'
AND countrylanguage.Language != 'Spanish' AND countrylanguage.CountryCode = country.Code;

