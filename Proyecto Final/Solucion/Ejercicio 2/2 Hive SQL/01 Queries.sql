-- a. Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4.

-- 26944
SELECT SUM(rentertripstaken) AS num_alquileres
FROM car_rental_analytics cra 
WHERE fuelType IN ('hybrid', 'electric') AND rating >= 4;


-- b. los 5 estados con menor cantidad de alquileres (crear visualización)

SELECT state_name, SUM(rentertripstaken) as num_alquileres
FROM car_rental_analytics cra
GROUP BY state_name 
ORDER BY num_alquileres ASC
LIMIT 5;

-- c. los 10 modelos (junto con su marca) de autos más rentados (crear visualización)

SELECT model, make, SUM(rentertripstaken) as num_alquileres
FROM car_rental_analytics cra
GROUP BY model , make
ORDER BY num_alquileres DESC
LIMIT 10;

-- d. Mostrar por año, cuántos alquileres se hicieron, teniendo en cuenta automóviles fabricados desde 2010 a 2015

SELECT cra.year, SUM(rentertripstaken) as num_alquileres
FROM car_rental_analytics cra
WHERE cra.year BETWEEN 2010 AND 2015
GROUP BY cra.year;

-- e. las 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o electrico)

SELECT city , SUM(rentertripstaken) AS num_alquileres
FROM car_rental_analytics cra 
WHERE fuelType IN ('hybrid', 'electric')
GROUP BY city
order by num_alquileres DESC 
LIMIT 5;

-- f. el promedio de reviews, segmentando por tipo de combustible

SELECT fueltype  , CAST(AVG(reviewcount) AS INT) AS avg_alquileres
FROM car_rental_analytics cra 
GROUP BY fueltype;

