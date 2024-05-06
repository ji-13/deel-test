select 
    external_ref::VARCHAR AS chargeback_external_ref
    , status::BOOLEAN AS is_accepted_chargeback
    , source::VARCHAR AS chargeback_source
    , chargeback::BOOLEAN AS is_chargeback
    from DEEL_TEST.PUBLIC.GLOBEPAY_CHARGEBACK_REPORT