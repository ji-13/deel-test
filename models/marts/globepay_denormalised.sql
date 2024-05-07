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
    , "GCR".chargeback_external_ref IS NULL AS is_missing_charegback_data

    , "GAR".currency_code
    , "GAR".transaction_amount AS transaction_amount_original
    {% for currency in currencies %}
    , round(rates_variant:{{currency}}*transaction_amount,2) as transaction_amount_{{currency}}
    {% endfor %}

    , CURRENT_TIMESTAMP() AS last_updated

from {{ ref('globepay_acceptance_report') }} "GAR"
LEFT JOIN {{ ref('globepay_chargeback_report') }} "GCR"
ON "GAR".external_ref = "GCR".chargeback_external_ref
{% if is_incremental() %}
  WHERE "GAR".transaction_datetime > (SELECT DATEADD('d', -1, MAX(transaction_datetime)) AS cutoff FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY "GAR".external_ref ORDER BY "GAR".transaction_datetime DESC NULLS LAST) = 1