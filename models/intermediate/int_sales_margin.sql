SELECT 
    a.*
    ,b.purchase_price * a.quantity AS COST
FROM 
    {{ref('stg_gz_raw_data__sales')}} as a
JOIN 
    {{ref('stg_gz_raw_data__product')}} as b

USING(products_id)
WHERE quantity > 1
order by quantity desc