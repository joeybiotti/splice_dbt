{{ config(materialized='table') }}

with unnnested_genres as (
    select
        movie_id,
        unnest(string_to_array(genres, '|')) as genre_name
    from {{ ref('int_movies_deduplicated') }}
)

select
    movie_id,
    md5(genre_name) as genre_key
from unnnested_genres