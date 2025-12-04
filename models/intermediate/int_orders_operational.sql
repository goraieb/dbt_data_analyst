WITH 

A AS (
    SELECT * FROM {{ ref('stg_gz_raw_data__ship') }}
), --SHIPPING DATA, HAS SHIPPING FEES ANS COSTS
B AS (
    SELECT * from {{ ref('int_orders_margin') }}
) -- ORDER DATA, INCLUDING REVENUE AND COGS

SELECT
    B.orders_id
    ,B.date_date
    ,ROUND((
            +SUM(B.margin)
            +SUM(A.shipping_fee)
            -SUM(A.logcost)
            -SUM(A.ship_cost)
            )
        ,2) AS operational_margin
    ,SUM(B.quantity) AS quantity
    ,SUM(B.margin) AS margin
    ,SUM(A.shipping_fee) as shipping_fee
    ,SUM(A.logcost) as log_cost
    ,SUM(A.ship_cost) as ship_cost
FROM B
LEFT JOIN A ON A.orders_id = B.orders_id
GROUP BY 
    B.orders_id
    ,B.date_date

ORDER BY   
    B.orders_id
    ,B.date_date

