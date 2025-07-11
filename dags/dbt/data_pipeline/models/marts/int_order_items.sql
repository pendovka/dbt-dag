select
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    line_item.discount,
    line_item.discount_percentage,
    orders.order_key,
    orders.order_date,
    orders.customer_key,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} as item_discount_amount
from 
    {{ ref('stg_tpch_line_items') }} as line_item
join
    {{ ref('stg_tpch_orders') }} as orders
on line_item.order_key = orders.order_key
order by
    orders.order_date