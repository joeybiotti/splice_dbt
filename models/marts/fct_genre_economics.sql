{{ config(materialized='table') }}

with genre_data as (
    select * from {{ ref('int_movies_genre_expanded') }}
),

final as (
    select
        genre_name,
        count(movie_id) as movie_count,
        cast(avg(gross_revenue) as bigint) as avg_revenue,
        cast(avg(budget) as bigint) as avg_budget,
        cast(sum(gross_revenue - budget) as bigint) as total_net_profits,
        --ROI Calculation
        round(sum(gross_revenue - budget) / nullif(sum(budget), 0), 2)
            as genre_roi
    from genre_data
    where
        gross_revenue is not NULL
        and budget is not NULL
    group by 1
)

select * from final
order by total_net_profits desc