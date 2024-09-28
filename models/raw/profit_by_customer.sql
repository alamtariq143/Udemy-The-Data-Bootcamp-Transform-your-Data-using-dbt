
SELECT C.CUSTOMERNAME, SUM(O.ORDERSELLINGPRICE - O.ORDERCOSTPRICE) AS PROFIT
FROM RAW.GLOBALMART.CUSTOMER C
LEFT JOIN RAW.GLOBALMART.ORDERS O
ON C.CUSTOMERID = O.CUSTOMERID
GROUP BY CUSTOMERNAME