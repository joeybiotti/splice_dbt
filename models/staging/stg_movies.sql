with source as (
    select * from {{ source('raw','movies')}}
),

renamed as (
    select 
        md5(
            coalesce(cast(movie_title as string), '_null_') || '-' || coalesce(cast(title_year as string), '_null_')
        ) as movie_id,
        trim(replace(movie_title, chr(160), '')) as movie_title,
        director_name, 
        country, 
        language, 
        try_cast(title_year as integer) as release_year, 
        cast(gross as bigint) as gross_revenue,
        cast(budget as bigint) as budget,
        cast(imdb_score as decimal(3,1)) as imdb_score,
        cast(duration as integer) as duration_minutes,
        cast(movie_facebook_likes as integer) as movie_facebook_likes, 
        string_split(genres, '|') as genre_list
    from source
)

select * from renamed 