select 
    external_ref::VARCHAR AS chargeback_external_ref
    , TRY_TO_BOOLEAN(status::TEXT) AS is_accepted_chargeback
    , source::VARCHAR AS chargeback_source
    , TRY_TO_BOOLEAN(chargeback::TEXT) AS is_chargeback
from {{ ref("stg_public__globepay_chargeback_report") }}
