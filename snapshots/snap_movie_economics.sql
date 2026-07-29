{% snapshot snap_movie_economics %}

{{
    config(
        target_schema='snapshots',
        unique_key='movie_id',
        strategy='check',
        check_cols=['gross_revenue', 'budget'],
    )
}}

select 
    movie_id,
    gross_revenue,
    budget
from {{ ref('stg_movies')}}

{% endsnapshot %}