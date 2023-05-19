{{ 
    config(
        materialized='incremental'
    ) 
}}

with final as (
    SELECT A1.user_id, A1.first_name, A1.last_name, A1.phone_number, A1.city, A1.state, A1.zip_code, A1.country, A2.transaction_date, A2.Total_Spent_2021, A2.Avg_Spent_2021, A2.Total_Spent_2022, A2.Avg_Spent_2022, A2.diff_sum_2021_minus_2022, A2.diff_avg_2021_minus_2022
    FROM {{ ref('avg_spend_merged') }} A2
    JOIN {{ ref('user_details') }} A1
    ON A2.user_id = A1.user_id 
)

SELECT * FROM FINAL
{% if is_incremental() %}
  where transaction_date > (select max(transaction_date) from {{ this }})
{% endif %}