-- dim_exercise : catalogue unique d'exercices.
--
-- Base = exercise_name deduplique de 600k_fitness_detailed (catalogue de
-- reference, PAS une source de faits). Les exercise_name de weight_training
-- sont resolus contre ce catalogue en 4 ETAPES EN CASCADE (chaque etape ne
-- s'applique qu'aux exercices non resolus par l'etape precedente) :
--
--   1. match STRICT sur normalized_exercise_name (egalite exacte apres
--      minuscule/ponctuation) -- le plus fiable.
--   2. match sur normalized_exercise_base_name (etape 1 + mots d'equipement
--      generiques retires + pluriel simplifie, cf.
--      normalize_exercise_base_name.sql) -- fait passer le taux de matching
--      de 38.3% a 64.2% a lui seul (voir GOLD_MODEL_DECISIONS.md).
--   3. FUZZY matching (rapidfuzz, calcule hors dbt par
--      scripts/fuzzy_match_exercises.py car dbt-postgres ne supporte pas les
--      modeles Python) sur normalized_exercise_base_name, seuil 85%. Les 2
--      candidats verifies manuellement comme faux positifs
--      ('glute extension' -> 'Leg Extension' ; 'low incline bench' ->
--      'Incline Bench Row') sont EXPLICITEMENT exclus ci-dessous.
--   4. mapping MANUEL (dbt/seeds/manual_exercise_muscle_mapping.csv) pour
--      les exercices les plus frequents restant non resolus apres l'etape 3,
--      avec justification par ligne.
--
-- Ce qui reste non resolu apres l'etape 4 recoit muscle_group='unknown',
-- is_matched=false -- volontairement : objectif non atteint = 100% de
-- matching, mais un taux residuel documente et honnete (voir
-- data/gold/GOLD_MODEL_DECISIONS.md, aucune ligne de fait n'est perdue).

with catalog_raw as (
    select
        normalized_exercise_name,
        normalized_exercise_base_name,
        exercise_name,
        count(*) as occurrence_count
    from {{ ref('stg_600k_fitness_detailed') }}
    group by normalized_exercise_name, normalized_exercise_base_name, exercise_name
),

catalog_representative_name as (
    select
        normalized_exercise_name,
        normalized_exercise_base_name,
        exercise_name,
        row_number() over (
            partition by normalized_exercise_name
            order by occurrence_count desc, exercise_name asc
        ) as rn
    from catalog_raw
),

-- Grain = 1 exercice DISTINCT du catalogue (normalized_exercise_name exact)
catalog_exercises as (
    select
        normalized_exercise_name,
        normalized_exercise_base_name,
        exercise_name,
        {{ classify_muscle_group('normalized_exercise_name') }} as muscle_group
    from catalog_representative_name
    where rn = 1
),

-- Grain = 1 nom de base DISTINCT (plusieurs exercices catalogue peuvent
-- partager le meme nom de base apres retrait de l'equipement -- on retient
-- le muscle_group le plus frequent parmi eux, coherent puisque le mouvement
-- de base est generalement identique entre variantes d'equipement).
catalog_by_base_name as (
    select
        normalized_exercise_base_name,
        mode() within group (order by muscle_group) as muscle_group
    from catalog_exercises
    where normalized_exercise_base_name is not null and normalized_exercise_base_name != ''
    group by normalized_exercise_base_name
),

weight_training_raw as (
    select
        normalized_exercise_name,
        normalized_exercise_base_name,
        exercise_name,
        count(*) as occurrence_count
    from {{ ref('stg_weight_training') }}
    group by normalized_exercise_name, normalized_exercise_base_name, exercise_name
),

weight_training_representative_name as (
    select
        normalized_exercise_name,
        normalized_exercise_base_name,
        exercise_name,
        row_number() over (
            partition by normalized_exercise_name
            order by occurrence_count desc, exercise_name asc
        ) as rn
    from weight_training_raw
),

weight_training_exercises as (
    select normalized_exercise_name, normalized_exercise_base_name, exercise_name
    from weight_training_representative_name
    where rn = 1
),

-- Etape 1 : exercices weight_training SANS correspondance stricte (grain
-- normalized_exercise_name exact) -- ce sont ceux qui ont besoin des etapes 2/3/4.
unresolved_after_strict as (
    select w.normalized_exercise_name, w.normalized_exercise_base_name, w.exercise_name
    from weight_training_exercises w
    left join catalog_exercises c on w.normalized_exercise_name = c.normalized_exercise_name
    where c.normalized_exercise_name is null
),

-- Etape 3 : candidats fuzzy retenus (seuil >=85%), faux positifs exclus
-- explicitement apres verification manuelle sur un echantillon (voir
-- data/gold/GOLD_MODEL_DECISIONS.md, section "Verification de l'echantillon fuzzy").
fuzzy_candidates as (
    select weight_training_normalized_base_name, matched_catalog_normalized_base_name
    from {{ source('raw', 'fuzzy_exercise_matches') }}
    where similarity_score >= 85
      and weight_training_normalized_base_name not in ('glute extension', 'low incline bench')
),

manual_mapping as (
    select normalized_exercise_base_name, manual_muscle_group
    from {{ ref('manual_exercise_muscle_mapping') }}
),

resolved_after_strict as (
    select
        u.normalized_exercise_name,
        u.exercise_name,
        coalesce(base_match.muscle_group, fuzzy_match.muscle_group, manual_mapping.manual_muscle_group, 'unknown') as muscle_group,
        case
            when base_match.muscle_group is not null then 'base_name_match'
            when fuzzy_match.muscle_group is not null then 'fuzzy_match'
            when manual_mapping.manual_muscle_group is not null then 'manual_mapping'
            else 'unmatched'
        end as match_stage
    from unresolved_after_strict u
    left join catalog_by_base_name base_match
        on u.normalized_exercise_base_name = base_match.normalized_exercise_base_name
    left join fuzzy_candidates fc
        on base_match.normalized_exercise_base_name is null
       and u.normalized_exercise_base_name = fc.weight_training_normalized_base_name
    left join catalog_by_base_name fuzzy_match
        on fc.matched_catalog_normalized_base_name = fuzzy_match.normalized_exercise_base_name
    left join manual_mapping
        on base_match.normalized_exercise_base_name is null
       and fc.weight_training_normalized_base_name is null
       and u.normalized_exercise_base_name = manual_mapping.normalized_exercise_base_name
),

combined as (
    select
        normalized_exercise_name,
        exercise_name,
        muscle_group,
        'catalog_strict' as match_stage,
        true as is_matched,
        'catalog_600k_fitness' as source
    from catalog_exercises

    union all

    select
        normalized_exercise_name,
        exercise_name,
        muscle_group,
        match_stage,
        (match_stage != 'unmatched') as is_matched,
        'weight_training_' || match_stage as source
    from resolved_after_strict
)

select
    row_number() over (order by normalized_exercise_name) as exercise_id,
    exercise_name,
    normalized_exercise_name,
    muscle_group,
    cast(null as varchar) as equipment,
    is_matched,
    match_stage,
    source
from combined
