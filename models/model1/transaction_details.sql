{{ 
    config(
        materialized='incremental',
        unique_key='SPEND.spend_id',
        merge_update_columns = ['TRANSACTIONS.amount'],
    ) 
}}

with final as (
    SELECT S.spend_id, T.transaction_id, T.transaction_date, T.amount, S.product_id, S.user_id, T.created_at 
    FROM TRANSACTIONS T
    JOIN SPEND S
    ON S.transaction_id = T.transaction_id
)

select * from final
{% if is_incremental() %}
  where created_at > (select max(created_at) from {{ this }})
{% endif %}