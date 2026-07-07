-- dim_date : calendrier genere sur la plage de dates couverte par
-- weight_training.performed_at (seule table source ayant des dates
-- exploitables pour fact_workout_session). Cle naturelle = la date elle-meme
-- (type date), plus simple et plus lisible qu'une cle de substitution
-- entiere pour une dimension calendaire.

with date_bounds as (
    select
        min(session_date) as min_date,
        max(session_date) as max_date
    from {{ ref('stg_weight_training') }}
),

date_spine as (
    select generate_series(min_date, max_date, interval '1 day')::date as date_day
    from date_bounds
)

select
    date_day as date_id,
    date_day,
    extract(day from date_day)::int as day_of_month,
    extract(dow from date_day)::int as day_of_week,  -- convention Postgres : 0=dimanche ... 6=samedi
    extract(week from date_day)::int as week_of_year,
    extract(month from date_day)::int as month,
    extract(year from date_day)::int as year,
    date_trunc('week', date_day)::date as week_start_date
from date_spine
