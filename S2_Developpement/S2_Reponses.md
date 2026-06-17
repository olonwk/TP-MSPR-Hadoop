# RÉPONSES AUX QUESTIONS DE RÉFLEXION — SÉANCE 2
## FinData Solutions | DEPE855

---

### Question 1 — Comment avez-vous géré la dépendance entre le Flux 2 et le Flux 1 ? Quel mécanisme garantit que le référentiel de devises est disponible avant la transformation des transactions ?

**Réponse :**

La dépendance est gérée à **deux niveaux distincts** :

**Niveau 1 — Orchestration Airflow (garantie structurelle)**

Dans le DAG `dag_findata_pipeline.py`, les tâches sont liées par des dépendances explicites :

```python
ingest_flux2 >> transform_flux2
[transform_flux2, ingest_flux1] >> transform_flux1
```

Cette ligne signifie que `transform_flux1` ne démarrera **jamais** tant que `transform_flux2` n'est pas en état `SUCCESS`. Airflow surveille cet état et ne planifie pas la tâche aval tant que l'état de la tâche amont n't est pas `SUCCESS`. Si `transform_flux2` échoue, `transform_flux1` passe en état `upstream_failed` et une alerte email est envoyée à l'équipe.

**Niveau 2 — Vérification applicative dans le job Spark (garantie défensive)**

Dans `flux1_transactions.py`, avant toute transformation, on vérifie que le référentiel est peuplé pour la date du jour :

```python
df_cours = spark.read.parquet(HDFS_REF_COURS).filter(col("date_partition") == date_traitement)
nb_devises = df_cours.count()
if nb_devises == 0:
    raise RuntimeError(f"Référentiel vide pour {date_traitement}...")
```

Cette double vérification (orchestration + applicative) constitue une défense en profondeur : même si Airflow avait un bug de scheduling, le job Spark lui-même refuserait de produire des données incorrectes (montants en EUR nuls ou erronés).

**Cas de gestion du délai du fichier SFTP :**

Un `BashOperator` avec retry (3 tentatives, délai 5 min) est configuré pour `ingest_flux2_sftp`. Si les 3 tentatives échouent, une tâche de fallback copie le référentiel de la veille et le job Flux 1 peut tourner avec les taux J-1, tout en produisant un champ `source=FALLBACK` dans les métadonnées.

---

### Question 2 — Quel est l'impact du partitionnement HDFS sur les performances des requêtes PySpark de l'équipe Data Science ? Donnez un exemple concret.

**Réponse :**

Le partitionnement HDFS par `annee/mois/jour` active le mécanisme de **partition pruning** dans le moteur Spark/Catalyst. Lorsqu'une requête comporte un filtre sur une colonne de partition, Spark **n'ouvre pas les répertoires HDFS** correspondant aux partitions non filtrées — aucun I/O, aucune allocation mémoire pour ces données.

**Exemple concret :**

L'équipe Data Science souhaite entraîner un modèle de détection de fraude sur les transactions de janvier 2024 uniquement :

```python
df = spark.read.parquet("/production/transactions/") \
         .filter((col("annee") == 2024) & (col("mois") == 1))
```

**Sans partitionnement (CSV ou Parquet plat) :**
- Volume total sur 3 ans : 3 × 365 × 500 000 lignes = **547 millions de lignes, ~1,2 To**
- Spark lit 100% des données, décompresse, filtre en mémoire
- Durée estimée : **~45 minutes** sur 4 executors

**Avec partitionnement annee/mois/jour :**
- Spark consulte d'abord le NameNode HDFS pour lister les répertoires
- Seuls les répertoires `annee=2024/mois=01/jour=01/` à `jour=31/` sont ouverts
- Volume lu : **31 × 500 000 lignes = 15,5 millions de lignes, ~34 Go**
- Durée estimée : **~2 minutes** — réduction de **97% des données lues**

**Deuxième bénéfice — Predicate pushdown Parquet :**

Si la requête ajoute un filtre sur une colonne non partitionnée (ex: `code_banque = 'BNP'`), Spark utilise les statistiques min/max stockées dans les métadonnées Parquet de chaque fichier (row group statistics) pour sauter les blocs qui ne contiennent pas `BNP`. Ce mécanisme est transparent pour le data scientist mais peut réduire encore les données lues de 60 à 80%.

**Recommandation pour l'équipe Data Science :**

Toujours utiliser les colonnes de partition en premier dans les filtres. Éviter les expressions non-déterministes sur les colonnes de partition (ex: `year(to_date(col("annee_str")))` empêche le pruning — utiliser directement `col("annee")`).
