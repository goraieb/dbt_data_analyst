WITH C AS  (
SELECT 
    a.*
    ,CAST(b.purchase_price AS NUMERIC)  * a.quantity AS cogs
FROM 
    {{ref('stg_gz_raw_data__sales')}} as a
JOIN 
    {{ref('stg_gz_raw_data__product')}} as b

USING(products_id)
)

SELECT 
    *
    , round(revenue - cogs,2) as product_margin
FROM C