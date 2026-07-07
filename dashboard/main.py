"""SafeLift — API de serving (etape 5/6).

Expose les donnees Gold (schema `gold` de app-postgres, construites par dbt
en etape 4) pour le dashboard de visualisation du risque
musculo-squelettique. Perimetre volontairement limite : pas de streaming
Kafka, pas de nutrition, pas de ML — uniquement lecture des tables
gold.fact_risk_score / gold.fact_risk_score_demo_synthetic / gold.dim_user
deja calculees par le pipeline dbt.

Connexion Postgres : psycopg2 (meme bibliotheque que
spark/jobs/load_silver_to_postgres.py et scripts/fuzzy_match_exercises.py,
pour rester coherent avec le reste du projet), via un petit pool de
connexions (le service reste mono-instance, un pool simple suffit).
"""

import os
from contextlib import contextmanager
from datetime import date

import psycopg2
import psycopg2.pool
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

import risk_formula

app = FastAPI(title="SafeLift Dashboard", version="0.3.0")

# Libelles anatomiques francais : DUPLIQUES depuis MUSCLE_LABELS_FR de
# dashboard/static/dashboard.js (petit dictionnaire statique, pas assez
# volumineux pour justifier une source de verite partagee JSON/config -- si
# MUSCLE_LABELS_FR cote JS change, ce dictionnaire doit etre mis a jour en
# miroir, voir CLAUDE.md).
MUSCLE_LABELS_FR = {
    "shoulder": "Épaules / deltoïdes",
    "chest": "Pectoraux",
    "abs": "Abdominaux",
    "arms": "Bras (biceps/triceps)",
    "legs": "Cuisses (quadriceps)",
    "knee": "Genoux",
    "back": "Haut du dos",
    "lower_back": "Bas du dos / lombaires",
    "unknown": "Zone non classifiée",
    "calves": "Mollets",
}

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

DB_CONFIG = {
    "host": os.environ.get("APP_POSTGRES_HOST", "app-postgres"),
    "port": int(os.environ.get("APP_POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("APP_POSTGRES_DB", "safelift_dwh"),
    "user": os.environ.get("APP_POSTGRES_USER", "safelift_app"),
    "password": os.environ.get("APP_POSTGRES_PASSWORD", "change_me_app"),
}

# Pool minimal (service mono-instance, faible trafic attendu pour une demo) :
# evite de rouvrir une connexion TCP a chaque requete sans la complexite d'un
# pool applicatif plus lourd.
connection_pool = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=5, **DB_CONFIG)


@contextmanager
def get_cursor():
    """Fournit un curseur Postgres (dict par ligne) et rend proprement la connexion au pool."""
    conn = connection_pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
    finally:
        connection_pool.putconn(conn)


@app.get("/health")
def health() -> dict:
    """Endpoint de verification de sante du service."""
    return {"status": "ok"}


@app.get("/")
def dashboard_page() -> FileResponse:
    """Sert la page HTML du dashboard (silhouette + selecteurs)."""
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/users")
def list_users() -> dict:
    """Liste des user_id reels de dim_user (jamais les scenarios synthetiques),
    separee en deux groupes explicites plutot qu'une liste plate.

    Rappel important (voir data/gold/GOLD_MODEL_DECISIONS.md, section 5) :
    un seul profil (le "demo user") porte reellement des donnees de seance
    dans gold.fact_risk_score -- les 972 autres profils gym_members n'en ont
    aucune (aucune cle commune reelle entre les 2 datasets sources). Une
    simple liste plate (comme dans la version precedente de cette API)
    laissait le dropdown du dashboard atterrir par defaut, la plupart du
    temps, sur un profil sans donnee -- premier coup d'oeil sur un ecran
    vide pour un jury. La structure {users_with_data, users_without_data}
    force le dashboard a toujours pre-selectionner un profil AVEC donnees.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            select du.user_id, du.age, du.gender, du.experience_level, du.is_weight_training_demo_user
            from gold.dim_user du
            where exists(select 1 from gold.fact_risk_score fr where fr.user_id = du.user_id)
            order by du.user_id
            """
        )
        users_with_data = cur.fetchall()

        cur.execute(
            """
            select du.user_id, du.age, du.gender, du.experience_level, du.is_weight_training_demo_user
            from gold.dim_user du
            where not exists(select 1 from gold.fact_risk_score fr where fr.user_id = du.user_id)
            order by du.user_id
            """
        )
        users_without_data = cur.fetchall()

    return {"users_with_data": users_with_data, "users_without_data": users_without_data}


@app.get("/users/{user_id}/risk")
def get_user_risk(user_id: int) -> dict:
    """Dernier risk_score par muscle_group pour un utilisateur, avec tous les facteurs.

    "Dernier" = ligne la plus recente (session_date) pour chaque zone
    musculaire distincte -- pas juste le score agrege, chaque facteur
    (base_zone, charge_factor, volume_factor, recup_factor, duree_factor)
    reste visible pour que le dashboard puisse justifier "pourquoi" le score
    est ce qu'il est (voir data/gold/GOLD_MODEL_DECISIONS.md, section
    fact_risk_score : "pas de boite noire").
    """
    with get_cursor() as cur:
        cur.execute("select 1 from gold.dim_user where user_id = %s", (user_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail=f"user_id {user_id} introuvable")

        cur.execute(
            """
            select distinct on (mu.muscle_group)
                mu.muscle_group,
                fr.risk_score,
                fr.risk_level,
                fr.base_zone,
                fr.charge_factor,
                fr.volume_factor,
                fr.recup_factor,
                fr.duree_factor,
                fr.session_date,
                fr.workout_session_id,
                ex.exercise_name
            from gold.fact_risk_score fr
            join gold.dim_muscle mu on fr.muscle_id = mu.muscle_id
            join gold.dim_exercise ex on fr.exercise_id = ex.exercise_id
            where fr.user_id = %s
            order by mu.muscle_group, fr.session_date desc, fr.workout_session_id desc
            """,
            (user_id,),
        )
        muscles = cur.fetchall()

    return {"user_id": user_id, "muscles": muscles, "is_synthetic_demo": False}


@app.get("/users/{user_id}/risk/history")
def get_user_risk_history(user_id: int) -> dict:
    """Historique du risk_score moyen par date, pour une courbe de tendance simple.

    Moyenne (toutes zones confondues) par session_date : une seule serie
    simple a tracer, suffisant pour une demo (pas de decomposition par zone
    ici, qui necessiterait une legende multi-courbes hors perimetre de
    cette etape).
    """
    with get_cursor() as cur:
        cur.execute("select 1 from gold.dim_user where user_id = %s", (user_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail=f"user_id {user_id} introuvable")

        cur.execute(
            """
            select
                session_date,
                round(avg(risk_score), 2) as avg_risk_score,
                count(*) as data_points
            from gold.fact_risk_score
            where user_id = %s
            group by session_date
            order by session_date
            """,
            (user_id,),
        )
        history = cur.fetchall()

    return {"user_id": user_id, "history": history, "is_synthetic_demo": False}


@app.get("/demo/scenarios")
def list_demo_scenarios() -> list[dict]:
    """Les 9 scenarios synthetiques (3 par seuil Faible/Modere/Eleve).

    Chaque objet retourne is_synthetic_demo=true (colonne deja presente en
    base) : le dashboard doit s'appuyer sur ce champ pour ne JAMAIS melanger
    ces lignes avec les vraies donnees utilisateur -- voir l'avertissement
    dans dbt/models/marts/fact_risk_score_demo_synthetic.sql et
    data/gold/GOLD_MODEL_DECISIONS.md.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            select
                scenario_id,
                scenario_label,
                muscle_group,
                base_zone,
                charge_factor,
                volume_factor,
                recup_factor,
                duree_factor,
                raw_risk_score,
                risk_score,
                risk_level,
                notes,
                is_synthetic_demo
            from gold.fact_risk_score_demo_synthetic
            order by scenario_id
            """
        )
        return cur.fetchall()


@app.get("/users/{user_id}/exercises")
def list_user_exercises(user_id: int) -> dict:
    """Exercices REELLEMENT deja logues par cet utilisateur (distinct de
    gold.fact_workout_session), pour peupler le selecteur du simulateur
    what-if.

    Volontairement restreint aux exercices deja pratiques (pas le catalogue
    complet des 3 177 exercice de dim_exercise) : le simulateur what-if a
    besoin d'une moyenne historique reelle comme point de comparaison
    (charge_factor/volume_factor) -- un exercice jamais pratique par cet
    utilisateur n'aurait aucune baseline, rendant la simulation peu
    pertinente. `POST /api/simulate-risk` reste neanmoins robuste a un
    exercise_id jamais pratique (facteurs neutres, voir sa docstring), au
    cas ou l'endpoint serait appele directement hors du dashboard.
    """
    with get_cursor() as cur:
        cur.execute("select 1 from gold.dim_user where user_id = %s", (user_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail=f"user_id {user_id} introuvable")

        cur.execute(
            """
            select distinct ex.exercise_id, ex.exercise_name, ex.muscle_group
            from gold.fact_workout_session fws
            join gold.dim_exercise ex on fws.exercise_id = ex.exercise_id
            where fws.user_id = %s
            order by ex.exercise_name
            """,
            (user_id,),
        )
        exercises = cur.fetchall()

    return {"user_id": user_id, "exercises": exercises}


class SimulateRiskRequest(BaseModel):
    user_id: int
    exercise_id: int
    charge_kg: float
    reps: int
    sets: int
    duration_minutes: float


@app.post("/api/simulate-risk")
def simulate_risk(payload: SimulateRiskRequest) -> dict:
    """Simulateur what-if (Feature A) : risque predit pour un exercice +
    parametres hypothetiques, SANS RIEN ECRIRE EN BASE. Formule deterministe
    partagee avec dbt (voir dashboard/risk_formula.py pour le detail complet
    des constantes/deviations documentees vs dbt/models/marts/fact_risk_score.sql).

    Baselines (charge_factor/volume_factor) : moyenne reelle de l'utilisateur
    pour CET EXERCICE precis (gold.fact_workout_session) ; si cet exercice
    n'a jamais ete pratique par cet utilisateur, repli sur la moyenne de la
    ZONE musculaire (tous exercices confondus) -- repli documente dans
    l'"explication" du facteur concerne, jamais silencieux. Si aucune des
    deux baselines n'existe, facteur neutre (1.0), egalement explique.

    recup_factor : utilise la VRAIE derniere session_date deja loguee pour
    cette zone chez cet utilisateur (gold.fact_workout_session), comparee a
    la date reelle d'aujourd'hui -- rien n'est invente. Voir
    risk_formula.compute_recup_factor pour la limite honnete de ce calcul
    sur ce dataset (derniere seance reelle : 2018-09-29).

    risk_score_actuel : dernier gold.fact_risk_score.risk_score deja calcule
    par dbt pour CET EXERCICE precis chez cet utilisateur (repli sur la zone
    si cet exercice n'a jamais ete loggue) -- permet de comparer directement
    l'hypothese a une valeur deja verifiee en base (voir DoD de cette
    fonctionnalite).
    """
    with get_cursor() as cur:
        cur.execute("select 1 from gold.dim_user where user_id = %s", (payload.user_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail=f"user_id {payload.user_id} introuvable")

        cur.execute(
            """
            select ex.exercise_id, ex.exercise_name, ex.muscle_group, mu.muscle_id, mu.base_epidemiological_risk
            from gold.dim_exercise ex
            join gold.dim_muscle mu on ex.muscle_group = mu.muscle_group
            where ex.exercise_id = %s
            """,
            (payload.exercise_id,),
        )
        exercise = cur.fetchone()
        if exercise is None:
            raise HTTPException(status_code=404, detail=f"exercise_id {payload.exercise_id} introuvable")

        muscle_id = exercise["muscle_id"]
        muscle_group = exercise["muscle_group"]
        base_zone = float(exercise["base_epidemiological_risk"])

        cur.execute(
            """
            select avg(lifted_weight_kg) as avg_charge, avg(total_reps) as avg_volume, count(*) as n
            from gold.fact_workout_session
            where user_id = %s and exercise_id = %s
            """,
            (payload.user_id, payload.exercise_id),
        )
        exercise_baseline = cur.fetchone()

        if exercise_baseline["n"] > 0:
            baseline_source = "exercise"
            avg_charge = float(exercise_baseline["avg_charge"])
            avg_volume = float(exercise_baseline["avg_volume"])
        else:
            cur.execute(
                """
                select avg(lifted_weight_kg) as avg_charge, avg(total_reps) as avg_volume, count(*) as n
                from gold.fact_workout_session
                where user_id = %s and muscle_id = %s
                """,
                (payload.user_id, muscle_id),
            )
            muscle_baseline = cur.fetchone()
            if muscle_baseline["n"] > 0:
                baseline_source = "muscle_group"
                avg_charge = float(muscle_baseline["avg_charge"])
                avg_volume = float(muscle_baseline["avg_volume"])
            else:
                baseline_source = None
                avg_charge = None
                avg_volume = None

        cur.execute(
            """
            select max(session_date) as last_date
            from gold.fact_workout_session
            where user_id = %s and muscle_id = %s
            """,
            (payload.user_id, muscle_id),
        )
        last_session_date = cur.fetchone()["last_date"]

        cur.execute(
            """
            select risk_score from gold.fact_risk_score
            where user_id = %s and exercise_id = %s
            order by session_date desc, workout_session_id desc
            limit 1
            """,
            (payload.user_id, payload.exercise_id),
        )
        actuel_row = cur.fetchone()
        if actuel_row is None:
            cur.execute(
                """
                select risk_score from gold.fact_risk_score
                where user_id = %s and muscle_id = %s
                order by session_date desc, workout_session_id desc
                limit 1
                """,
                (payload.user_id, muscle_id),
            )
            actuel_row = cur.fetchone()
        risk_score_actuel = float(actuel_row["risk_score"]) if actuel_row else None

    total_reps_hypothetique = payload.sets * payload.reps
    duration_seconds_hypothetique = payload.duration_minutes * 60

    charge_factor, charge_pct = risk_formula.compute_charge_factor(payload.charge_kg, avg_charge)
    volume_factor, volume_ratio = risk_formula.compute_volume_factor(total_reps_hypothetique, avg_volume)
    recup_factor, hours_since_last = risk_formula.compute_recup_factor(last_session_date, date.today())
    duree_factor = risk_formula.compute_duree_factor(duration_seconds_hypothetique)

    _, risk_score_simule, risk_level_simule = risk_formula.compute_risk_score(
        base_zone, charge_factor, volume_factor, recup_factor, duree_factor
    )
    risk_level_actuel = risk_formula.risk_level_from_score(risk_score_actuel) if risk_score_actuel is not None else None
    delta = round(risk_score_simule - risk_score_actuel, 2) if risk_score_actuel is not None else None

    baseline_note = "" if baseline_source == "exercise" else " (moyenne de la zone : cet exercice précis n'a jamais été loggé par cet utilisateur)"

    if avg_charge is None:
        charge_explication = "Aucun historique de charge disponible (ni pour cet exercice, ni pour cette zone) — facteur neutre (1.0) appliqué."
    else:
        sign = "+" if charge_pct >= 0 else ""
        charge_explication = (
            f"Charge {sign}{charge_pct * 100:.0f}% vs moyenne habituelle{baseline_note} "
            f"({avg_charge:.1f}kg -> {payload.charge_kg:g}kg)"
        )

    if avg_volume is None:
        volume_explication = "Aucun historique de volume disponible (ni pour cet exercice, ni pour cette zone) — facteur neutre (1.0) appliqué."
    else:
        sign = "+" if volume_ratio >= 1 else ""
        volume_explication = (
            f"Volume (séries × répétitions) à {volume_ratio * 100:.0f}% de la moyenne habituelle{baseline_note} "
            f"({avg_volume:.0f} -> {total_reps_hypothetique} répétitions totales)"
        )

    if last_session_date is None:
        recup_explication = "Aucune séance déjà enregistrée sur cette zone pour cet utilisateur — facteur neutre (1.0) appliqué."
    elif hours_since_last < risk_formula.RECUP_THRESHOLD_HOURS:
        recup_explication = f"Dernière sollicitation de cette zone il y a {hours_since_last}h (le {last_session_date}) — moins de 48h, pénalité appliquée."
    else:
        recup_explication = f"Dernière sollicitation de cette zone le {last_session_date} ({hours_since_last // 24} jours) — récupération suffisante."

    if duree_factor > risk_formula.DUREE_FACTOR_NEUTRAL:
        duree_explication = f"Durée hypothétique de {payload.duration_minutes:g} min (> 2h) — pénalité appliquée."
    else:
        duree_explication = f"Durée hypothétique de {payload.duration_minutes:g} min (≤ 2h) — pas de pénalité."

    muscle_label = MUSCLE_LABELS_FR.get(muscle_group, muscle_group)

    return {
        "user_id": payload.user_id,
        "exercise_id": payload.exercise_id,
        "exercise_name": exercise["exercise_name"],
        "muscle_group": muscle_group,
        "muscle_zone": muscle_label,
        "risk_score_simule": risk_score_simule,
        "risk_level_simule": risk_level_simule,
        "risk_score_actuel": risk_score_actuel,
        "risk_level_actuel": risk_level_actuel,
        "delta": delta,
        "facteurs": {
            "base_zone": {
                "valeur": base_zone,
                "explication": f"Risque de base fixe de la zone {muscle_label} (caractéristique de la zone, indépendant du comportement).",
            },
            "charge_factor": {"valeur": charge_factor, "explication": charge_explication},
            "volume_factor": {"valeur": volume_factor, "explication": volume_explication},
            "recup_factor": {"valeur": recup_factor, "explication": recup_explication},
            "duree_factor": {"valeur": duree_factor, "explication": duree_explication},
        },
    }
