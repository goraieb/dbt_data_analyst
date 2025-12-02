SELECT 
    a.*
    ,CAST(b.purchase_price AS NUMERIC) * a.quantity AS purchase_cost
FROM
    {{ ref('stg_gz_raw_data__sales') }} AS a
JOIN
    {{ ref('stg_gz_raw_data__product') }} AS b
USING (products_id)
