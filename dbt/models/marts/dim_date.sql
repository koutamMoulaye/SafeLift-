-- dim_date : calendrier genere sur la plage de dates couverte par les
-- sources de fact_workout_session -- historiquement weight_training.performed_at
-- uniquement ; depuis le Jalon 2 sous-etape 3/5 (seances temps reel),
-- ETENDU pour couvrir aussi realtime_user_sessions.performed_at.
--
-- BUG REEL rencontre et corrige en testant cette sous-etape : sans cette
-- extension, une seance saisie aujourd'hui (2026) tombe hors de la plage
-- couverte par weight_training (2015-2018) -> le LEFT JOIN sur date_id dans
-- fact_workout_session.sql renvoie NULL -> week_start_date NULL dans
-- fact_risk_score.sql -> le LEFT JOIN sur week_start_date (deux NULL ne
-- sont jamais egaux en SQL) ne matche plus la ligne agregee correspondante
-- -> charge_factor/volume_factor NULL -> risk_score NULL pour TOUTE seance
-- temps reel, silencieusement. Corrige en unionnant les deux sources AVANT
-- de calculer min/max -- voir data/gold/GOLD_MODEL_DECISIONS.md section 11.
--
-- Cle naturelle = la date elle-meme (type date), plus simple et plus
-- lisible qu'une cle de substitution entiere pour une dimension calendaire.

with combined_dates as (
    select session_date from {{ ref('stg_weight_training') }}
    union all
    select session_date from {{ ref('stg_realtime_user_sessions') }}
),

date_bounds as (
    select
        min(session_date) as min_date,
        max(session_date) as max_date
    from combined_dates
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
