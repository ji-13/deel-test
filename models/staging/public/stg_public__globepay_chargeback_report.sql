with 

source as (

    select * from {{ source('public', 'globepay_chargeback_report') }}

),

renamed as (

    select
        external_ref,
        status,
        source,
        chargeback

    from source

)

select * from renamed
