{{ 
    config(
        materialized='incremental',
        unique_key='movie_id',
        incremental_strategy='merge'
        ) 
}}

select
    movie_id,
    budget,
    gross_revenue,
    loaded_at,
    (gross_revenue - budget) as total_profit,
    case
        when budget > 0 then (gross_revenue - budget) / budget
    end as roi
from {{ ref('int_movies_deduplicated') }}

{% if is_incremental() %}
where loaded_at > (select max(loaded_at) from {{this}})
{% endif %}