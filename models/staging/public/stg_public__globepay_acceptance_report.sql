with 

source as (

    select * from {{ source('public', 'globepay_acceptance_report') }}

),

renamed as (

    select
        external_ref,
        status,
        source,
        ref,
        date_time,
        state,
        cvv_provided,
        amount,
        country,
        currency,
        rates

    from source

)

select * from renamed
