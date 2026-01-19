with source as (
    select * from {{ source('raw', 'movies') }}
),

renamed as (
    select
        -- Create ID for downstream models
        md5(
            coalesce(cast(movie_title as varchar), '')
            || coalesce(cast(director_name as varchar), '')
            || coalesce(cast(title_year as varchar), '')
        ) as movie_id,

        -- Text columns
        cast(movie_title as varchar) as movie_title,
        cast(director_name as varchar) as director_name,
        cast(color as varchar) as color,
        cast(content_rating as varchar) as content_rating,
        cast(genres as varchar) as genres,
        cast(tile_year as integer) as title_year,

        -- Numeric columns
        cast(duration as integer) as duration,
        cast(gross as bigint) as gross_revenue,
        cast(budget as bigint) as budget,
        cast(num_voted_users as integer) as num_voted_users,
        cast(imdb_score as float) as imdb_score,
        cast(aspect_ratio as float) as aspect_ration

    from source

)

select * from renamed;