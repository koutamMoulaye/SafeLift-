-- fact_workout_session : une ligne = un exercice realise dans une seance.
-- Sources UNIFIEES (Jalon 2, sous-etape 3/5) : weight_training (Kaggle,
-- historique statique, reparti entre PLUSIEURS demo_users depuis
-- l'extension multi-profils du 2026-07-11) + realtime_user_sessions
-- (saisies utilisateur temps reel via le dashboard). L'union des deux
-- sources (avec l'agregation par-set -> par-exercice de weight_training,
-- ET l'assignation du user_id reel par bloc chronologique) est faite EN
-- AMONT, au niveau staging (stg_workout_sessions_unified.sql) -- voir ce
-- modele pour le detail complet et data/gold/GOLD_MODEL_DECISIONS.md
-- section 11 pour la decision d'architecture. 600k_fitness n'est TOUJOURS
-- pas une source de faits (uniquement le catalogue dim_exercise, decision
-- d'architecture inchangee).
--
-- EXTENSION MULTI-PROFILS (2026-07-11) : `unified.user_id` est desormais
-- TOUJOURS reel et non-null (les deux sources -- weight_training ET
-- realtime -- portent un vrai user_id des le staging). Le
-- `cross join demo_user` + `coalesce(u.user_id, du.user_id)` utilises
-- AVANT cette extension (necessaires quand `unified.user_id` pouvait etre
-- NULL pour weight_training, rattache a un seul demo_user ici) sont donc
-- SUPPRIMES -- simplification directe, pas juste une preference de style :
-- un cross join contre un `dim_user` filtre sur 5 lignes desormais
-- (`is_weight_training_demo_user`) aurait multiplie chaque ligne par 5,
-- ce qui aurait ete un bug reel (duplication x5 de fact_workout_session).
--
-- Grain d'agregation retenu : (user_id, session_date, workout_name,
-- normalized_exercise_name) -- user_id AJOUTE au grain avec cette
-- sous-etape (avant, tous les faits appartenaient au meme demo_user, le
-- grain (session_date, workout_name, normalized_exercise_name) suffisait ;
-- desormais plusieurs utilisateurs reels peuvent contribuer des seances, le
-- grain doit donc les distinguer explicitement). PAS exercise_name brut :
-- deux variantes textuelles d'un meme exercice (ex. casse differente) qui
-- normalisent vers la meme valeur doivent fusionner en une seule ligne,
-- sans quoi elles produiraient deux lignes distinctes partageant pourtant
-- le meme exercise_id apres jointure sur dim_exercise -- violation du
-- grain, detectee par tests/assert_fact_workout_session_grain_unique.sql.
--
-- Colonnes issues de l'agregation par-set de weight_training (voir
-- stg_workout_sessions_unified.sql pour le detail, deja au bon grain pour
-- la source realtime) :
--   - sets          = nombre de sets loggees pour cet exercice ce jour-la
--   - reps          = repetitions moyennes PAR SET (arrondi), format usuel
--                      "3x10" plutot qu'un total brut
--   - total_reps    = somme exacte des repetitions (colonne technique
--                      supplementaire, necessaire au calcul EXACT du
--                      volume_factor dans fact_risk_score -- sets x reps
--                      moyen introduirait une approximation)
--   - lifted_weight_kg = poids moyen souleve sur les sets de cet exercice
--                         ce jour-la ("charge de travail" representative)
--   - duration_seconds = somme des durees (rarement non-nul sur weight_training,
--                         voir data/silver/CLEANING_LOG.md ; toujours
--                         renseigne par le formulaire temps reel)
--
-- Jointures en LEFT JOIN (pas INNER) : dim_exercise couvre par construction
-- 100% des exercise_name de weight_training (catalogue + non-matches
-- ajoutes), donc aucun orphelin n'est attendu de cette source -- mais un
-- LEFT JOIN + tests not_null (schema.yml) font ECHOUER le pipeline
-- explicitement si ce n'etait plus le cas, plutot que de faire disparaitre
-- silencieusement des lignes via un INNER JOIN. Meme garde-fou pour la
-- source realtime (limite assumee : un exercice saisi en temps reel
-- inconnu de dim_exercise resterait orphelin -- voir
-- GOLD_MODEL_DECISIONS.md section 11 pour la mitigation cote formulaire).

with unified as (
    select * from {{ ref('stg_workout_sessions_unified') }}
)

select
    row_number() over (
        order by u.user_id, u.session_date, u.workout_name, u.normalized_exercise_name
    ) as workout_session_id,
    ex.exercise_id,
    mu.muscle_id,
    u.user_id,
    dt.date_id,
    u.session_date,
    u.workout_name,
    u.sets,
    u.reps,
    u.total_reps,
    u.lifted_weight_kg,
    u.duration_seconds
from unified u
left join {{ ref('dim_exercise') }} ex
    on u.normalized_exercise_name = ex.normalized_exercise_name
left join {{ ref('dim_muscle') }} mu
    on ex.muscle_group = mu.muscle_group
left join {{ ref('dim_date') }} dt
    on u.session_date = dt.date_id
