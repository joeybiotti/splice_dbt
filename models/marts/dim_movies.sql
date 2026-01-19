{{ config(materialized='table') }}

select
    movie_id,
    movie_title,
    title_year,
    director_name,
    content_rating
from {{ ref("int_movies_deduplicated") }}