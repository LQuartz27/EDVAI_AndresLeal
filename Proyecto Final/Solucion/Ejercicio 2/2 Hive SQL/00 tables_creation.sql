CREATE DATABASE IF NOT EXISTS car_rental_db;

USE car_rental_db;

CREATE TABLE IF NOT EXISTS car_rental_db.car_rental_analytics(fuelType STRING, 
                                                              rating INT, 
                                                              renterTripsTaken INT, 
                                                              reviewCount INT, 
                                                              city STRING, 
                                                              state_name STRING, 
                                                              owner_id INT, 
                                                              rate_daily INT, 
                                                              make STRING, 
                                                              model STRING, 
                                                              year INT) 
COMMENT 'Contiene data reportada en los informes del ministerio'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';';


-- TO SEE THE TABLE SCHEMA AFTER IT'S CREATED
DESCRIBE FORMATTED car_rental_analytics;