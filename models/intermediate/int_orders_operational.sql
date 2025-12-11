{{ config(materialized = 'view') }}

with orders_margin as (
    select *
    from {{ ref('int_orders_margin') }}
),

ship as (
    select *
    from {{ ref('stg_gz_raw_data__ship') }}
)

select
    o.orders_id,
    o.date_date,

    -- operational_margin = margin + shipping_fee - log_cost - ship_cost
    o.margin
        + s.shipping_fee
        - s.logcost
        - s.ship_cost         as operational_margin
from orders_margin o
left join ship s
    on o.orders_id = s.orders_id