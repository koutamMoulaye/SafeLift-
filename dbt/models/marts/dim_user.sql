-- dim_user : profils d'utilisateurs de la plateforme (973 lignes attendues,
-- un profil = un user_id, source unique = gym_members).
--
-- Colonne is_weight_training_demo_user : marque les profils auxquels les
-- 9 142 lignes de fact_workout_session (issues de weight_training) sont
-- rattachees. C'est une HYPOTHESE DE DEMONSTRATION, pas une jointure de
-- donnees reelle : weight_training (journal personnel de seances sur ~3 ans)
-- ne contient AUCUN identifiant utilisateur, et aucune cle commune n'existe
-- entre les deux jeux de donnees sources. Voir
-- data/gold/GOLD_MODEL_DECISIONS.md pour la justification complete.
--
-- ⚠️ EXTENSION MULTI-PROFILS (2026-07-11) : AVANT cette extension, un SEUL
-- profil portait ce flag (selection deterministe par experience_level /
-- workout_frequency_days_per_week / user_id, voir historique git). Decision
-- actee : repartir l'historique reel sur 5 profils distincts (blocs
-- chronologiques CONTIGUS, voir dbt/seeds/demo_user_blocks_seed.csv et
-- stg_workout_sessions_unified.sql) pour une demonstration plus riche. Le
-- flag reste un booleen simple (colonne INCHANGEE) mais vaut desormais
-- `true` pour les 5 user_id du seed au lieu d'un seul -- **aucune
-- modification de schema necessaire**, seule la logique de selection
-- change ici.
--
-- Critere de selection des 5 profils (100% deterministe, documente pour
-- reproductibilite) :
--   1. experience_level maximum (=3) uniquement -- coherent avec un
--      historique d'entrainement de plusieurs annees, tel que celui de
--      weight_training.
--   2. Parmi ceux-ci, workout_frequency_days_per_week maximum (=5)
--      uniquement.
--   3. Trie par user_id croissant, puis ALTERNANCE STRICTE DE GENRE en
--      parcourant cette liste (le genre du profil precedent ne peut pas se
--      repeter consecutivement) jusqu'a 5 profils retenus -- vise une
--      diversite de genre pour une demo plus representative (ex: 3
--      profils demo, uniquement des femmes de 18-19 ans, aurait ete moins
--      parlant en soutenance). Resultat concret sur ce dataset : user_id
--      9 (18, Female), 21 (18, Male), 34 (19, Female), 46 (19, Male), 83
--      (21, Female) -- 3 Female / 2 Male. Limite honnetement constatee :
--      la diversite d'AGE reste faible (18-21 ans) -- consequence directe
--      du tri de stg_gym_members.sql (`order by age, gender, ...`), qui
--      fait que les user_id les plus bas (candidats naturels du critere
--      1/2 ci-dessus) sont aussi les plus jeunes. Non corrige (hors
--      perimetre de cette extension, qui porte sur la repartition de
--      l'historique, pas sur le critere de selection des profils) --
--      documente honnetement plutot que masque.
--   user_id=9 (l'ancien demo_user unique) est INTENTIONNELLEMENT conserve
--   parmi les 5 -- continuite avec les captures/tests deja realises sur ce
--   profil (dashboard-v2, silhouette, what-if) avant cette extension. Son
--   contenu reel CHANGE neanmoins (voir plus bas) : il ne porte plus
--   l'integralite de l'historique 2015-2018, seulement le 1er bloc
--   chronologique (2015-10-23 -> 2016-07-30).
--
-- Ces 5 user_id et leurs bornes de dates assignees sont figes dans
-- dbt/seeds/demo_user_blocks_seed.csv (source unique de verite, relue ici
-- ET par stg_workout_sessions_unified.sql pour ne jamais dupliquer cette
-- liste).

with users as (
    select * from {{ ref('stg_gym_members') }}
),

demo_user_ids as (
    select distinct user_id from {{ ref('demo_user_blocks_seed') }}
)

select
    u.user_id,
    u.age,
    u.gender,
    u.body_weight_kg,
    u.height_m,
    u.max_bpm,
    u.avg_bpm,
    u.resting_bpm,
    u.session_duration_hours,
    u.calories_burned,
    u.workout_type,
    u.fat_percentage,
    u.water_intake_liters,
    u.workout_frequency_days_per_week,
    u.experience_level,
    u.bmi,
    (d.user_id is not null) as is_weight_training_demo_user
from users u
left join demo_user_ids d on u.user_id = d.user_id
