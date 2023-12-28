--a) Cuántos hombres y cuántas mujeres sobrevivieron

SELECT sex, SUM(survived) as supervivientes
FROM titanic_transformed tt 
GROUP BY sex
;

/*
sex     supervivientes
female	233
male	109
*/

--b) Cuántas personas sobrevivieron según cada clase (Pclass)

SELECT pclass, SUM(survived) as supervivientes
FROM titanic_transformed tt 
GROUP BY pclass
;

/*
pclass supervivientes
1	   136
2	   87
3	   119
*/

--c) Cuál fue la persona de mayor edad que sobrevivió -> Una persona de 80 años de edad

SELECT max(age) as max_age
FROM titanic_transformed tt 
WHERE survived = 1
;

--d) Cuál fue la persona más joven que sobrevivió  --> Un bebé -> 0 años

SELECT min(age) as min_age
FROM titanic_transformed tt 
WHERE survived = 1
;