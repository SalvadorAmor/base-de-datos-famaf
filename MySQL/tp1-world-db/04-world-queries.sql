
SELECT Name, Population FROM city ORDER BY Population DESC LIMIT 10;

SELECT Name FROM country WHERE IndepYear IS NOT NULL LIMIT 50;

UPDATE countrylanguage SET Percentage = 100.0 WHERE CountryCode = 'AIA' AND Language = 'English';