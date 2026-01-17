WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'raw',
            'movies'
        )}}
),
cleaned AS (
    SELECT
        *,
        -- 1. Aggressive cleaning for the "Logical" Key
        TRIM(REPLACE(movie_title, CHR(160), '')) AS clean_title,
        TRIM(
            REPLACE(COALESCE(director_name, 'Unknown'), CHR(160), '')
        ) AS clean_director,
        COALESCE(TRY_CAST(title_year AS INTEGER), 0) AS clean_year
    FROM
        source
),
deduped AS (
    SELECT
        *
    FROM
        cleaned qualify ROW_NUMBER() over (
            PARTITION BY clean_title,
            clean_director,
            clean_year
            ORDER BY
                num_voted_users DESC,
                movie_facebook_likes DESC
        ) = 1
),
FINAL AS (
    SELECT
        MD5(
            COALESCE(clean_title, '') || COALESCE(clean_director, '')|| COALESCE(cast(clean_year AS varchar), '0')
        ) AS movie_id,
        clean_title AS movie_title,
        director_name,
        country,
        LANGUAGE,
        TRY_CAST(title_year AS INTEGER) AS release_year, 
        CAST(gross AS bigint) AS gross_revenue,
        CAST(budget AS bigint) AS budget,
        CAST(imdb_score AS DECIMAL(3, 1)) AS imdb_score,
        CAST(DURATION AS INTEGER) AS duration_minutes,
        CAST(
            movie_facebook_likes AS INTEGER
        ) AS movie_facebook_likes,
        string_split(genres, '|') AS genre_list
    FROM
        deduped
)
SELECT
    *
FROM
    FINAL