{{ 
    config(
        materialized='incremental'
    ) 
}}

with final as (
    SELECT A1.user_id, A1.transaction_date, A1.Total_spends AS Total_Spent_2021, A1.avg_amount AS Avg_Spent_2021, A2.Total_spends AS Total_Spent_2022, A2.avg_amount  AS Avg_Spent_2022, A1.Total_spends - A2.Total_spends AS diff_sum_2021_minus_2022, A1.avg_amount - A2.avg_amount AS diff_avg_2021_minus_2022
    FROM {{ ref('avg_spend_2021') }} A1
    JOIN {{ ref('avg_spend_2022') }} A2
    ON A1.user_id = A2.user_id
    AND EXTRACT(MONTH FROM A1.transaction_date) = EXTRACT(MONTH FROM A2.transaction_date)
)

SELECT * FROM FINAL
{% if is_incremental() %}
  where transaction_date > (select max(transaction_date) from {{ this }})
{% endif %}