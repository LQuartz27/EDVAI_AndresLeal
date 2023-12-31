CREATE OR REPLACE VIEW clientes_vw 
AS
SELECT O.CUSTOMER_ID, C.COMPANY_NAME, SUM(OD.QUANTITY) AS PRODUCTOS_VENDIDOS
FROM ORDERS O
INNER JOIN ORDER_DETAILS OD 
  ON O.ORDER_ID = OD.ORDER_ID 
LEFT JOIN CUSTOMERS C 
  ON O.CUSTOMER_ID = C.CUSTOMER_ID 
GROUP BY O.CUSTOMER_ID, C.COMPANY_NAME
ORDER BY 3 DESC;