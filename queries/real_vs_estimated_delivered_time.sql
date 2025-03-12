-- TODO: Esta consulta devolverá una tabla con las diferencias entre los tiempos 
-- reales y estimados de entrega por mes y año. Tendrá varias columnas: 
-- month_no, con los números de mes del 01 al 12; month, con las primeras 3 letras 
-- de cada mes (ej. Ene, Feb); Year2016_real_time, con el tiempo promedio de 
-- entrega real por mes de 2016 (NaN si no existe); Year2017_real_time, con el 
-- tiempo promedio de entrega real por mes de 2017 (NaN si no existe); 
-- Year2018_real_time, con el tiempo promedio de entrega real por mes de 2018 
-- (NaN si no existe); Year2016_estimated_time, con el tiempo promedio estimado 
-- de entrega por mes de 2016 (NaN si no existe); Year2017_estimated_time, con 
-- el tiempo promedio estimado de entrega por mes de 2017 (NaN si no existe); y 
-- Year2018_estimated_time, con el tiempo promedio estimado de entrega por mes 
-- de 2018 (NaN si no existe).
-- PISTAS:
-- 1. Puedes usar la función julianday para convertir una fecha a un número.
-- 2. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
-- 3. Considera tomar order_id distintos.


SELECT 
    strftime('%m', order_purchase_timestamp) AS month_no,
    CASE 
        WHEN strftime('%m', order_purchase_timestamp) = '01' THEN 'Ene'
        WHEN strftime('%m', order_purchase_timestamp) = '02' THEN 'Feb'
        WHEN strftime('%m', order_purchase_timestamp) = '03' THEN 'Mar'
        WHEN strftime('%m', order_purchase_timestamp) = '04' THEN 'Abr'
        WHEN strftime('%m', order_purchase_timestamp) = '05' THEN 'May'
        WHEN strftime('%m', order_purchase_timestamp) = '06' THEN 'Jun'
        WHEN strftime('%m', order_purchase_timestamp) = '07' THEN 'Jul'
        WHEN strftime('%m', order_purchase_timestamp) = '08' THEN 'Ago'
        WHEN strftime('%m', order_purchase_timestamp) = '09' THEN 'Sep'
        WHEN strftime('%m', order_purchase_timestamp) = '10' THEN 'Oct'
        WHEN strftime('%m', order_purchase_timestamp) = '11' THEN 'Nov'
        WHEN strftime('%m', order_purchase_timestamp) = '12' THEN 'Dic'
    END AS month,
    AVG(CASE WHEN strftime('%Y', order_purchase_timestamp) = '2016' THEN 
        julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp) END) AS Year2016_real_time,
    AVG(CASE WHEN strftime('%Y', order_purchase_timestamp) = '2017' THEN 
        julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp) END) AS Year2017_real_time,
    AVG(CASE WHEN strftime('%Y', order_purchase_timestamp) = '2018' THEN 
        julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp) END) AS Year2018_real_time,
    AVG(CASE WHEN strftime('%Y', order_purchase_timestamp) = '2016' THEN 
        julianday(order_estimated_delivery_date) - julianday(order_purchase_timestamp) END) AS Year2016_estimated_time,
    AVG(CASE WHEN strftime('%Y', order_purchase_timestamp) = '2017' THEN 
        julianday(order_estimated_delivery_date) - julianday(order_purchase_timestamp) END) AS Year2017_estimated_time,
    AVG(CASE WHEN strftime('%Y', order_purchase_timestamp) = '2018' THEN 
        julianday(order_estimated_delivery_date) - julianday(order_purchase_timestamp) END) AS Year2018_estimated_time
FROM olist_orders
WHERE order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL
GROUP BY month_no
ORDER BY month_no;
