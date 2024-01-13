CREATE DATABASE IF NOT EXISTS ejercicio1_db;

USE ejercicio1_db;

CREATE TABLE IF NOT EXISTS ejercicio1_db.vuelos(fecha DATE, 
                                                horaUTC STRING, 
                                                clase_de_vuelo STRING, 
                                                clasificacion_de_vuelo STRING, 
                                                tipo_de_movimiento STRING, 
                                                aeropuerto STRING, 
                                                origen_destino STRING, 
                                                aerolinea_nombre STRING, 
                                                aeronave STRING, 
                                                pasajeros INT) 
COMMENT 'Contiene data reportada en los informes del ministerio'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';';

CREATE TABLE IF NOT EXISTS ejercicio1_db.aeropuertos_detalles(aeropuerto STRING, 
                                                              oac STRING, 
                                                              iata STRING, 
                                                              tipo STRING, 
                                                              denominacion STRING, 
                                                              coordenadas STRING, 
                                                              latitud STRING, 
                                                              longitud STRING, 
                                                              elev FLOAT, 
                                                              uom_elev STRING, 
                                                              ref STRING, 
                                                              distancia_ref FLOAT, 
                                                              direccion_ref STRING, 
                                                              condicion STRING, 
                                                              control STRING, 
                                                              region STRING, 
                                                              uso STRING, 
                                                              trafico STRING, 
                                                              sna STRING, 
                                                              concesionado STRING, 
                                                              provincia STRING)
COMMENT 'Contiene detalles de los aeropuertos'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';';

-- TO SEE THE TABLE SCHEMA AFTER THEY ARE CREATED
DESCRIBE FORMATTED vuelos;
DESCRIBE FORMATTED aeropuertos_detalles;