/*
6.	Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022. Mostrar consulta y 
Resultado de la query
*/

SELECT COUNT(*) as conteo
FROM vuelos v
WHERE FECHA >= '2021-12-01' AND FECHA < '2022-02-01'
;

/*
7.	Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022. 
Mostrar consulta y Resultado de la query
*/

SELECT SUM(v.pasajeros) AS CANTIDAD_PASAJEROS
FROM vuelos v
INNER JOIN aeropuertos_detalles ad
ON v.aeropuerto = ad.aeropuerto
WHERE FECHA >= '2021-01-01' AND FECHA < '2022-07-01'
;

/*
8.	Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto de arribo, ciudad de arribo, 
y cantidad de pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados por fecha de manera descendiente. 
Mostrar consulta y Resultado de la query
*/

SELECT * FROM
(
	SELECT
	v.fecha ,
	v.horautc ,
	v.aeropuerto as aeropuerto_salida,
	ad1.`ref` as ciudad_salida,
	v.origen_destino as aeropuerto_llegada,
	ad2.`ref` as ciudad_llegada
	FROM vuelos v
	INNER JOIN aeropuertos_detalles ad1
	    ON v.aeropuerto = ad1.aeropuerto
	INNER JOIN aeropuertos_detalles ad2
	    ON v.origen_destino = ad2.aeropuerto
	WHERE v.fecha >= '2021-01-01' AND v.fecha < '2022-07-01'
	AND v.tipo_de_movimiento = 'Despegue'
	
	UNION 
	
	
	SELECT
	v.fecha ,
	v.horautc ,
	v.origen_destino as aeropuerto_salida,
	ad2.`ref` as ciudad_salida,
	v.aeropuerto as aeropuerto_llegada,
	ad1.`ref` as ciudad_llegada
	FROM vuelos v
	INNER JOIN aeropuertos_detalles ad1
	    ON v.aeropuerto = ad1.aeropuerto
	INNER JOIN aeropuertos_detalles ad2
	    ON v.origen_destino = ad2.aeropuerto
	WHERE v.fecha >= '2021-01-01' AND v.fecha < '2022-07-01'
	AND v.tipo_de_movimiento = 'Aterrizaje'
) FULL_QUERY
ORDER BY fecha DESC
;

/*
9.	Cuáles son las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el 30/06/2022 
exceptuando aquellas aerolíneas que no tengan nombre. Mostrar consulta y Visualización
*/

SELECT
v.aerolinea_nombre,
SUM(v.pasajeros) AS CANTIDAD_PASAJEROS
FROM vuelos v
WHERE FECHA >= '2021-01-01' AND FECHA < '2022-07-01'
AND v.aerolinea_nombre <> '0'
GROUP BY v.aerolinea_nombre
ORDER BY CANTIDAD_PASAJEROS DESC
LIMIT 10
;

/*
10.	Cuáles son las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 que despegaron desde 
la Ciudad autónoma de Buenos Aires o de Buenos Aires, exceptuando aquellas aeronaves que no cuentan con nombre. 
Mostrar consulta y Visualización
*/

SELECT
v.aeronave,
SUM(v.pasajeros) AS CANTIDAD_PASAJEROS
FROM vuelos v
INNER JOIN aeropuertos_detalles ad
ON v.aeropuerto = ad.aeropuerto
WHERE FECHA >= '2021-01-01' AND FECHA < '2022-07-01'
AND v.aeronave <> '0'
AND v.tipo_de_movimiento = 'Despegue'
AND ad.provincia in ('BUENOS AIRES', 'CIUDAD AUTÓNOMA DE BUENOS AIRES')
GROUP BY v.aeronave
ORDER BY CANTIDAD_PASAJEROS DESC
LIMIT 10
;

