{{ 
    config(
        materialized='incremental',
        unique_key='spend_id'
    ) 
}}

with final as (
    SELECT user_id, transaction_date,
       sum(amount) OVER (PARTITION BY user_id, EXTRACT(MONTH FROM created_at)) AS Total_spends, 
       avg(amount) OVER (PARTITION BY user_id, EXTRACT(MONTH FROM created_at)) AS avg_amount,
       product_id, spend_id
    FROM {{ ref('transaction_details_2021') }}
)

SELECT user_id, transaction_date, ROUND(Total_spends,2) as Total_spends, ROUND(avg_amount,2) as avg_amount, product_id, spend_id
FROM FINAL
{% if is_incremental() %}
  where transaction_date > (select max(transaction_date) from {{ this }})
{% endif %}