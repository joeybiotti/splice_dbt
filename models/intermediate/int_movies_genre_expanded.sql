with deduplicated_movies as (
    select * from {{ ref('int_movies_deduplicated') }}
),

expanded as (
    select
        movie_id,
        gross_revenue,
        budget,
        unnest(string_split(genres, '|')) as genre_name
    from deduplicated_movies
)

select * from expanded