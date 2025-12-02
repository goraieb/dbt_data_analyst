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
    ,sum(B.quantity) AS quantity
FROM B
LEFT JOIN A ON A.orders_id = B.orders_id

GROUP BY 
    B.orders_id
    ,B.date_date

ORDER BY   
    B.orders_id
    ,B.date_date

