{{ config(materialized = 'view') }}

-- STEP 1: Rebuild per-order data exactly like step 4

with orders_margin as (
    select *
    from {{ ref('int_orders_margin') }}
),

ship as (
    select *
    from {{ ref('stg_gz_raw_data__ship') }}
),

orders_operational as (
    select *
    from {{ ref('int_orders_operational') }}
),

per_order as (
    select
        o.date_date,
        o.orders_id,
        o.revenue,
        o.quantity,
        o.purchase_cost,
        o.margin,
        s.shipping_fee,
        s.logcost,
        s.ship_cost,
        oo.operational_margin
    from orders_margin o
    left join ship s
        on o.orders_id = s.orders_id
    left join orders_operational oo
        on o.orders_id = oo.orders_id
        and o.date_date = oo.date_date
),

-- STEP 2: Your original structure (daily aggregation)
orders_per_day as (
    select
        date_date,
        count(distinct orders_id)              as nb_transactions,
        round(sum(revenue), 0)                 as revenue,
        round(sum(margin), 0)                  as margin,
        round(sum(operational_margin), 0)      as operational_margin,
        round(sum(purchase_cost), 0)           as purchase_cost,
        round(sum(shipping_fee), 0)            as shipping_fee,
        round(sum(logcost), 0)                 as log_cost,
        round(sum(ship_cost), 0)               as ship_cost,
        sum(quantity)                           as quantity
    from per_order
    group by date_date
)

-- STEP 3: Final finance_days output
select
    date_date,
    revenue,
    margin,
    operational_margin,
    purchase_cost,
    shipping_fee,
    log_cost,
    ship_cost,
    quantity,
    round(revenue / nullif(nb_transactions, 0), 2) as average_basket
from orders_per_day
order by date_date desc