# Read.me
See below for the simplified flow of the two data sources provided - Chargebacks and Acceptances. The sources are accessed through a connection to Snowflake, and freshness of the date_time transaction field is checked. The sources are then renamed and cleaned, with preventative casting and qualifying to maintain data integrity. The schema.yml file also specifies out-of-the-box tests to check the uniqueness/non-null quality of the primary keys and that the relation between the two sources exists. 
The two building blocks are then joined together to make a final denormalised mart for analyst use which brings together the transaction and chargeback data using the external_reference. 

```mermaid
flowchart TD
    A[Snowflake Sources] -->B(public.globepay_chargeback_report)
    A[Snowflake Sources] -->C(public.globepay_acceptance_report)
    C -->|renaming| D[Modelled globepay_acceptance_report]
    B -->|renaming| E[Modelled globepay_chargeback_report]
    D -->|joining, transforming| F[Denormalised mart]
    E -->|joining, transforming| F[Denormalised mart]
```
# Table Definitions
`globepay_denormalised` is the final mart table which is incremental, which means that new entries from the source data are deleted and inserted based on the date specification. 
Column definitions:

| Column Name    | Definition |
| -------- | ------- |
| EXTERNAL_REF  | Primary key of the transaction, payment parameters (like amount, country, and currency) together with the input details collected from the customer    |
| REFERENCE | Identifcation field     |
| COUNTRY_CODE    | 2-Character code to identify the country of the transaction    |
| TRANSACTION_DATE    | Truncated Date of Transaction   |
| TRANSACTION_DATETIME    | Date and Timestamp of Transaction    |
| SOURCE    | GLOBEPAY   |
| TRANSACTION_STATE    | ACCEPTED or DENIED  |
| IS_ACCEPTED    | BOOLEAN field, transcation is accepted    |
| IS_CVV_PROVIDED    | BOOLEAN field, card verification provided by customer    |
| IS_ACCEPTED_CHARGEBACK    | BOOLEAN field, true if acceptance field is true from chargeback data source    |
| IS_CHARGEBACK    | BOOLEAN field, true if field is true from chargeback data source    |
| IS_MISSING_CHAREGBACK_DATA    | BOOLEAN field, true if the external_ref does not exist the chargeback data source    |
| CURRENCY_CODE    | 3-Character code to identify the currency    |
| TRANSACTION_AMOUNT_ORIGINAL    | Transaction amount in local currency    |
| TRANSACTION_AMOUNT_USD    | Transaction amount in USD    |
| TRANSACTION_AMOUNT_GBP    | Transaction amount in GBP      |
| TRANSACTION_AMOUNT_CAD    | Transaction amount in CAD      |
| TRANSACTION_AMOUNT_MXN    | Transaction amount in MXN      |
| TRANSACTION_AMOUNT_EUR    | Transaction amount in EUR      |
| LAST_UPDATED    | CURRENT_TIMESTAMP value of when the entry was last updated    |

# Example Analyses

1. What is the acceptance rate over time?
```
WITH acceptance_rates AS (
SELECT 
    TRANSACTION_DATE
    , SUM(IFF(is_accepted, 1, 0)) AS acceptance
    , SUM(1) AS totals
    , SUM(acceptance) OVER (ORDER BY
TRANSACTION_DATE ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last_7_days_acceptance
    , SUM(totals) OVER (ORDER BY
TRANSACTION_DATE ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last_7_days_total
    , SUM(acceptance) OVER (ORDER BY
TRANSACTION_DATE ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS last_30_days_acceptance
    , SUM(totals) OVER (ORDER BY
TRANSACTION_DATE ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS last_30_days_total
FROM DEEL_TEST.DBT_JI13.GLOBEPAY_DENORMALISED
group by 1
order by 1)

SELECT 
TRANSACTION_DATE
, last_7_days_acceptance/last_7_days_total AS rolling_7_day_acceptance_rate
, last_30_days_acceptance/last_30_days_total AS rolling_30_day_acceptance_rate
FROM acceptance_rates;
```
   
2. List the countries where the amount of declined transactions went over $25M

```
SELECT 
COUNTRY_NAME 
, SUM(transaction_amount_usd) AS total_transaction_amount_usd
FROM DEEL_TEST.DBT_JI13.GLOBEPAY_DENORMALISED
WHERE NOT is_accepted
GROUP BY  1
HAVING total_transaction_amount_usd > 25000000
ORDER BY 2 DESC;
```
   
3. Which transactions are missing chargeback data?
   
 ```
SELECT 
external_ref
FROM DEEL_TEST.DBT_JI13.GLOBEPAY_DENORMALISED
WHERE is_missing_charegback_data;
```
