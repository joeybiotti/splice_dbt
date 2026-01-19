{{ config(materialized='table') }}

select
    movie_id,
    budget,
    gross_revenue,
    (gross_revenue - budget) as total_profit,
    case
        when budget > 0 then (gross_revenue - budget) / budget
    end as roi
from {{ ref('int_movies_deduplicated') }}