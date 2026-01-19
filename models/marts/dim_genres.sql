{{ config(materialized='table') }}

with base_genres as (
    select unnest(string_to_array(genres, '|')::VARCHAR []) as genre_name
    from {{ ref('int_movies_deduplicated') }}
),

distinct_genres as (
    select distinct trim(genre_name) as genre_name
    from base_genres
    where genre_name is not null
)

select
    md5(genre_name) as genre_key,
    genre_name
from distinct_genres