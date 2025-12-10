with 

source as (

    select * from {{ source('gz_raw_data', 'product') }}

),

renamed as (

    select
        products_id,
        CAST(purchase_price, FLOAT64) AS purchase_price

    from source

)

select * from renamed