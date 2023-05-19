{{ 
    config(
        materialized='incremental',
        unique_key='spend_id'
    ) 
}}

with final as (
    SELECT *
    FROM {{ ref('transaction_details') }}
    WHERE EXTRACT(YEAR FROM created_at) = 2021
)

SELECT * FROM FINAL
{% if is_incremental() %}
  where created_at > (select max(created_at) from {{ this }})
{% endif %}