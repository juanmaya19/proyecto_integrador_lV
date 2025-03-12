SELECT 
    oi.order_id,
    SUM(oi.freight_value) AS total_freight_value,
    SUM(p.product_weight_g) AS total_weight_g
FROM olist_order_items oi
JOIN olist_orders o ON oi.order_id = o.order_id
JOIN olist_products p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY oi.order_id;
