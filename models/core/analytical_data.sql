{{ config(materialized='table') }}

with rsrch_data as (
    select * from {{ ref('fact_rsrch_all') }}
)
    select 
    hospital_or_entity_name,
    recipient_state,
    Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name,
    {{ dbt.date_trunc("month", "Date_of_Payment") }} as payment_month,
    sum(total_amount_payment_usdollars) as total_amount_payment,
    count(Record_ID) as total_records
    from rsrch_data
    group by 1,2,3,4