select
    external_ref::VARCHAR AS external_ref,
    ref::VARCHAR as reference,
    status::BOOLEAN as is_accepted,
    source::VARCHAR AS source,
    date_time::DATE as transaction_date,
    date_time::timestamp as transaction_datetime,
    state::VARCHAR as transaction_state,
    cvv_provided::BOOLEAN as is_cvv_provided,
    TRY_TO_NUMBER(amount, 38, 2) as transaction_amount,
    country::TEXT(2) as country_code,
    currency::TEXT(3) as currency_code,
    to_variant(parse_json(rates)) as rates_variant
from deel_test.public.globepay_acceptance_report
qualify row_number() over (partition by external_ref order by date_time::timestamp desc) = 1

