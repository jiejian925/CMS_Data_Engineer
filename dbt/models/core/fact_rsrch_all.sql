
{{
    config(
        materialized='table'
    )
}}

select * 
from {{ ref('stg_rsrch_all') }}