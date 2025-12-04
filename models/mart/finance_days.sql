


With m as 
(
    select * from {{ ref('int_orders_margin') }}
    ) -- table with orders info on product level
, p as ( 
    select * from {{ ref('int_orders_operational') }}
    ) -- orders operational 

SELECT
    m.date_date
    , COUNT(p.orders_id) as nb_transactions
    , round(sum(m.revenue),2) as total_revenue
    , round(safe_divide(sum(m.revenue),COUNT(m.orders_id)),2) as avg_basket
    , round(sum(p.operational_margin),2) as total_operational_margin
    , round(sum(m.purchase_cost),2) as Total_purchase_cost
    , round(sum(p.shipping_fee),2) as Total_shipping_fees
    , round(sum(p.log_cost),2) as Total_log_costs
    , round(sum(p.quantity),2) as quantity_of_products_sold
FROM p
JOIN m 
    USING(orders_id)
GROUP BY 
    m.date_date
ORDER BY 
    m.date_date