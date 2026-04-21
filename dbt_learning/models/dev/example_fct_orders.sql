-- models/dev/example_fct_orders.sql
-- This is an example fact model to guide you on Task 1.4

with orders as (

    select * from {{ ref('example_stg_orders') }}

),

-- Example of how you might aggregate or join in the DEV layer
final as (

    select
        order_status,
        count(order_id) as total_orders,
        sum(shipping_fee) as total_shipping_revenue

    from orders
    group by 1

)

select * from final
