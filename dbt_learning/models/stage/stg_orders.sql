-- STAGE · stg_orders
-- Clean, cast, and standardise the raw orders seed.

with source as (

    select * from {{ source('RAW', 'raw_orders') }}

),

cleaned as (

    select
        -- Primary key
        order_id::integer                               as order_id,

        -- Foreign keys
        trim(customer_id)::text                         as customer_id,

        -- Dates
        order_date::date                                as order_date,

        -- Status
        lower(trim(status))::text                       as order_status,

        -- Order details
        trim(store_id)::text                            as store_id,
        coalesce(shipping_fee, 0)::numeric(12,2)        as shipping_fee,

        -- Currency
        upper(trim(currency))::text                     as currency

    from source

)

select * from cleaned
