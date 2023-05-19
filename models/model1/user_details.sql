{{ 
    config(
        materialized='incremental',
        unique_key='user_id'
    ) 
}}

with final as (
    SELECT U.user_id, U.first_name, U.last_name, U.phone_number, A.city, A.state, A.zip_code, A.country, U.created_at
    FROM USERS U
    JOIN ADDRESS A
    ON U.address_id = A.address_id
)

SELECT * FROM FINAL
{% if is_incremental() %}
  where created_at > (select max(created_at) from {{ this }})
{% endif %}