{{
    config(
        materialized='incremental',
        unique_key='external_ref',
    )
}}

{% set currencies = ["USD", "GBP", "CAD", "MXN", "EUR"] %}

select
    "GAR".external_ref
    , "GAR".reference
    , "GAR".country_code
    , "GAR".transaction_date
    , "GAR".transaction_datetime
    , "GAR".source
    , "GAR".transaction_state
    , "GAR".is_accepted
    , "GAR".is_cvv_provided
    , "GCR".is_accepted_chargeback
    , "GCR".is_chargeback
    , "GAR".currency_code
    , "GAR".transaction_amount AS transaction_amount_original
    {% for currency in currencies %}
    , round(rates_variant:{{currency}}*transaction_amount,2) as transaction_amount_{{currency}}
    {% endfor %}

    , CURRENT_TIMESTAMP() AS last_updated

from {{ ref('GLOBEPAY_ACCEPTANCE_REPORT') }} "GAR"
LEFT JOIN {{ ref('GLOBEPAY_CHARGEBACK_REPORT') }} "GCR"
ON "GAR".external_ref = "GCR".chargeback_external_ref
{% if is_incremental() %}
  WHERE "GAR".transaction_datetime > (SELECT DATE_ADD(MAX(transaction_datetime) AS max_source_timestamp FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY "GAR".external_ref ORDER BY "GAR".transaction_datetime DESC NULLS LAST) = 1