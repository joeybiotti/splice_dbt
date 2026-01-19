with staging as (
    select * from {{ ref('stg_movies') }}
),

cleaned as (
    select
        *,
        trim(replace(movie_title, chr(160), '')) as clean_title
    from staging
),

deduplicated as (
    select * from cleaned
    qualify row_number() over (
        partition by clean_title, director_name, title_year
        order by num_voted_users desc
    ) = 1
)

select * from deduplicated