{{
    config(
        materialized='table',
        partition_by={
          "field": "date_of_payment",
          "data_type": "date",
          "granularity": "day"
        },
        cluster_by=['recipient_state', 'submitting_manufacturer_or_gpo_name', 'hospital_or_entity_name'],
        options={
          "require_partition_filter": True
        }
    )
}}

SELECT
  record_id,
  program_year,
  change_type,
  covered_recipient_type,
  hospital_or_entity_name,
  recipient_city,
  recipient_state,
  recipient_zip_code,
  recipient_country,
  pi_1_covered_recipient_type,
  pi_1_full_name,
  pi_1_city,
  pi_1_state,
  pi_1_zip_code,
  pi_1_primary_type_1,
  pi_1_specialty_1,
  submitting_manufacturer_or_gpo_name,
  payment_manufacturer_or_gpo_name,
  payment_manufacturer_or_gpo_state,
  related_product_indicator,
  covered_or_noncovered_indicator_1,
  indicate_product_type_1,
  product_category_or_therapeutic_area_1,
  product_name_1,
  total_amount_payment_usdollars,
  date_of_payment,
  form_of_payment,
  payment_publication_date
FROM
  {{ ref('stg_rsrch_all') }}