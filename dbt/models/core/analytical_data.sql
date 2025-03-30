{{ config(materialized='table') }}

with rsrch_data as (
    select * from {{ ref('fact_rsrch_all_partitioned_clustered') }}
)
    select 
        hospital_or_entity_name,
        recipient_state,
        submitting_manufacturer_or_gpo_name,
        {{ dbt.date_trunc("month", "date_of_payment") }} as payment_month,
        sum(total_amount_payment_usdollars) as total_amount_payment,
        count(record_id) as total_records
    from rsrch_data
    group by 1,2,3,4