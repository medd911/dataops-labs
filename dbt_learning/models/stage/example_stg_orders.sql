-- models/stage/example_stg_orders.sql
-- This is an example staging model to guide you on Task 1.3

with source as (

    -- Using ref() to reference the seed data directly
    select * from {{ ref('raw_orders') }}

),

cleaned as (

    select
        order_id::integer                               as order_id,
        trim(customer_id)::text                         as customer_id,
        order_date::date                                as order_date,
        lower(trim(status))::text                       as order_status,
        coalesce(shipping_fee, 0)::numeric(12,2)        as shipping_fee

    from source

)

select * from cleaned
