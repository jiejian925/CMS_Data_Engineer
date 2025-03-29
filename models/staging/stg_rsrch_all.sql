{{
    config(
        materialized='view'
    )
}}

with source_data as (

    select * from {{ source('staging','RSRCH_ALL') }}

),

renamed_casted as (
    select

        {{ dbt.safe_cast("Record_ID", api.Column.translate_type("string")) }} as record_id,
        {{ dbt.safe_cast("Program_Year", api.Column.translate_type("integer")) }} as program_year,

        -- Recipient Information
        {{ dbt.safe_cast("Change_Type", api.Column.translate_type("string")) }} as change_type,
        {{ dbt.safe_cast("Covered_Recipient_Type", api.Column.translate_type("string")) }} as covered_recipient_type,
        -- Combine hospital/entity names
        coalesce(
            nullif(trim({{ dbt.safe_cast("Noncovered_Recipient_Entity_Name", api.Column.translate_type("string")) }}), ''),
            nullif(trim({{ dbt.safe_cast("Teaching_Hospital_Name", api.Column.translate_type("string")) }}), '')
        ) as hospital_or_entity_name,
        {{ dbt.safe_cast("Recipient_City", api.Column.translate_type("string")) }} as recipient_city,
        {{ dbt.safe_cast("Recipient_State", api.Column.translate_type("string")) }} as recipient_state,
        {{ dbt.safe_cast("Recipient_Zip_Code", api.Column.translate_type("string")) }} as recipient_zip_code,
        {{ dbt.safe_cast("Recipient_Country", api.Column.translate_type("string")) }} as recipient_country,
        {{ dbt.safe_cast("Principal_Investigator_1_Covered_Recipient_Type", api.Column.translate_type("string")) }} as pi_1_covered_recipient_type,
        -- Combine first and last names, handle potential nulls
        trim(
            coalesce({{ dbt.safe_cast("Principal_Investigator_1_First_Name", api.Column.translate_type("string")) }}, '') || ' ' ||
            coalesce({{ dbt.safe_cast("Principal_Investigator_1_Last_Name", api.Column.translate_type("string")) }}, '')
        ) as pi_1_full_name,
        {{ dbt.safe_cast("Principal_Investigator_1_City", api.Column.translate_type("string")) }} as pi_1_city,
        {{ dbt.safe_cast("Principal_Investigator_1_State", api.Column.translate_type("string")) }} as pi_1_state,
        {{ dbt.safe_cast("Principal_Investigator_1_Zip_Code", api.Column.translate_type("string")) }} as pi_1_zip_code,
        {{ dbt.safe_cast("Principal_Investigator_1_Primary_Type_1", api.Column.translate_type("string")) }} as pi_1_primary_type_1,
        {{ dbt.safe_cast("Principal_Investigator_1_Specialty_1", api.Column.translate_type("string")) }} as pi_1_specialty_1,

        -- Manufacturer/GPO Information
        {{ dbt.safe_cast("Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name", api.Column.translate_type("string")) }} as submitting_manufacturer_or_gpo_name,
        {{ dbt.safe_cast("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name", api.Column.translate_type("string")) }} as payment_manufacturer_or_gpo_name,
        {{ dbt.safe_cast("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State", api.Column.translate_type("string")) }} as payment_manufacturer_or_gpo_state,

        -- Product Information
        {{ dbt.safe_cast("Related_Product_Indicator", api.Column.translate_type("string")) }} as related_product_indicator,
        {{ dbt.safe_cast("Covered_or_Noncovered_Indicator_1", api.Column.translate_type("string")) }} as covered_or_noncovered_indicator_1,
        {{ dbt.safe_cast("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1", api.Column.translate_type("string")) }} as indicate_product_type_1,
        {{ dbt.safe_cast("Product_Category_or_Therapeutic_Area_1", api.Column.translate_type("string")) }} as product_category_or_therapeutic_area_1,
        {{ dbt.safe_cast("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1", api.Column.translate_type("string")) }} as product_name_1,

        -- Payment Information
        {{ dbt.safe_cast("Total_Amount_of_Payment_USDollars", api.Column.translate_type("float")) }} as total_amount_payment_usdollars,
        -- Cast date fields to date type
        {{ dbt.safe_cast("Date_of_Payment", api.Column.translate_type("date")) }} as date_of_payment,
        {{ dbt.safe_cast("Form_of_Payment_or_Transfer_of_Value", api.Column.translate_type("string")) }} as form_of_payment,
        {{ dbt.safe_cast("Payment_Publication_Date", api.Column.translate_type("date")) }} as payment_publication_date

    from source_data
)

select * from renamed_casted

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}