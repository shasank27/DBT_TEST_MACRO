{% test newer_record(model, column_name, test0, test1) %}

{% set var1 = test0.split('|') %}
{% set var2 = test1.split('|') %}

with validation as (
    select *
    from {{ model }}
    where CAST({{ var1[0] }} AS Date) >= CAST({{ var1[1] }} AS Date)
    or {{ var2[0] }} is {{ var2[1] }}
)
valid

select *
from validation 

{% endtest %}