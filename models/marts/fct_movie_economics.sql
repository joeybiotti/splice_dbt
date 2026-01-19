{{ config(
    materialized='table'
) }}

with movies as (
    select * from {{ ref('int_movies_deduplicated') }}
),

final as (
    select
        movie_id,
        clean_title as movie_title,
        director_name,
        title_year,
        gross_revenue,
        budget,
        -- Calculated business logic
        (gross_revenue - budget) as net_profit,
        case
            when budget > 0 then (gross_revenue - budget) / budget
        end as return_on_investment,
        imdb_score,
        num_voted_users
    from movies
    where
        gross_revenue is not null
        and budget is not null
)

select * from final