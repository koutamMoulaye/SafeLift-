# SafeLift - Image Airflow personnalisee (ajoute les providers Kafka/Spark)
# Variante python3.10 (et non 3.12) choisie deliberement : l'image
# apache/spark:3.5.8-python3 utilisee par spark-master/spark-worker embarque
# Python 3.10 (version fixee par l'image officielle Apache, non modifiable
# sans reconstruire une image Spark custom). PySpark exige que driver et
# executeurs tournent sur la MEME version mineure de Python des qu'un UDF
# Python est utilise (cf. jobs silver_600k_fitness_summary/detailed qui
# parsent des colonnes via une UDF) : erreur PYTHON_VERSION_MISMATCH sinon.
# Aligner le cote Airflow (plus simple a changer, un seul Dockerfile) sur le
# Python 3.10 de l'image Spark officielle evite de devoir maintenir une image
# Spark personnalisee.
FROM apache/airflow:2.10.4-python3.10

# pyspark (installe via requirements.txt) a besoin d'une JVM pour fonctionner,
# meme cote driver (spark-submit en mode client) : l'image de base apache/airflow
# n'embarque aucun JRE. Java 17 est officiellement supporte par Spark 3.5.x.
# JAVA_HOME est calcule dynamiquement (pas de chemin en dur type
# .../java-17-openjdk-arm64) pour rester portable entre architectures
# (amd64/arm64) : update-alternatives connait le chemin reel une fois le
# paquet installe.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && echo "JAVA_HOME=$(dirname $(dirname $(readlink -f /usr/bin/java)))" >> /etc/environment
ENV JAVA_HOME=/usr/lib/jvm/default-java
RUN ln -sfn "$(dirname $(dirname $(readlink -f /usr/bin/java)))" /usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# /opt/dbt_venv doit appartenir a l'utilisateur airflow : l'image de base
# apache/airflow refuse explicitement les `pip install` executes en root
# (garde-fou impose par l'image officielle), donc la creation du venv et les
# installations pip qui suivent doivent se faire sous USER airflow.
RUN mkdir -p /opt/dbt_venv && chown -R airflow:root /opt/dbt_venv

USER airflow

# dbt-core et Airflow ont des contraintes de version conflictuelles sur des
# dependances communes (click, jinja2...) : les installer dans le MEME
# environnement Python est une source classique de rupture (recommandation
# officielle : isoler dbt dans son propre venv). On cree donc un venv dedie
# /opt/dbt_venv, entierement separe du site-packages d'Airflow. Il n'est PAS
# ajoute au PATH global (pour ne pas faire de l'ombre au pip/python3
# d'Airflow juste en dessous) : dbt est toujours invoque via son chemin
# complet /opt/dbt_venv/bin/dbt (voir airflow/dags/gold_dbt_run.py).
RUN pip install --no-cache-dir virtualenv \
    && python3 -m virtualenv /opt/dbt_venv \
    && /opt/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.8.2 dbt-postgres==1.8.2

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
