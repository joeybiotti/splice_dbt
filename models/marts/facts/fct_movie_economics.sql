{{
    config(
        materialized='incremental',
        unique_key='movie_id',
        incremental_strategy='merge'
    )
}}

with source as (
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
)

{% if is_incremental() %}

, max_loaded as (
    select max(loaded_at) as max_loaded_at from {{ this }}
)

select source.*
from source, max_loaded
where source.loaded_at > max_loaded.max_loaded_at

{% else %}

select * from source

{% endif %}