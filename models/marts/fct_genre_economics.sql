WITH staging AS (
    SELECT
        *
    FROM
        {{ ref('stg_movies') }}
),
exploded AS (
    SELECT
        unnest(genre_list) AS genre,
        movie_id,
        movie_title,
        director_name,
        imdb_score,
        budget,
        gross_revenue,
        (gross_revenue - budget) AS profit
    FROM
        staging
    WHERE
        budget IS NOT NULL
        AND gross_revenue IS NOT NULL
)
SELECT
    genre,
    COUNT(DISTINCT movie_id) AS movie_count,
    ROUND(AVG(imdb_score), 2) AS avg_imdb_score,
    ROUND(AVG(profit), 2) AS avg_profit_per_movie,
    SUM(gross_revenue) AS total_gross_revenue
FROM
    exploded
GROUP BY
    1
ORDER BY
    total_gross_revenue DESC