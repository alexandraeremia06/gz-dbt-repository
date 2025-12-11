{{ config(materialized = 'view') }}

with sales as (
    select *
    from {{ ref('stg_gz_raw_data__sales') }}
),

product as (
    select *
    from {{ ref('stg_gz_raw_data__product') }}
)

select
    s.date_date,
    s.orders_id,
    s.pdt_id,
    s.quantity,
    s.revenue,

    p.purchase_price,
    s.quantity * p.purchase_price as purchase_cost,

    -- margin = revenue - purchase_cost
    s.revenue - (s.quantity * p.purchase_price) as margin
from sales s
left join product p
    on s.pdt_id = p.products_id