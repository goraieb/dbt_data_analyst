WITH orders AS  (
select * from 
{{ ref('int_sales_margin') }}
)

SELECT 
    date_date
    , orders_id
    ,round(sum(revenue),2) as revenue
    ,round(sum(quantity),2) as quantity
    ,round(sum(cogs),2) as purchase_cost
    ,round(revenue - purchase_cost, 2) as margin
FROM orders
GROUP BY 
    date_date
    ,orders_id

ORDER BY 
    date_date
    ,orders_id



