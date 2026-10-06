# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake et architecture médaillon : cas Stripe
# MAGIC
# MAGIC **Objectif** : pratiquer, sur des paiements fictifs, ce qu'on a décidé dans l'architecture :
# MAGIC
# MAGIC | Étape du notebook | Lien avec l'architecture Stripe |
# MAGIC |---|---|
# MAGIC | 1. Générer des événements | Simule les messages Kafka issus de la table outbox (flux 2) |
# MAGIC | 2. Bronze | Flux 3 : on stocke le message brut, rien n'est perdu |
# MAGIC | 3. Silver | Flux 4 : parsing, validation, dédoublonnage par `MERGE` (idempotence) |
# MAGIC | 4. Gold | Schéma en étoile (livrable OLAP) + features (flux 6) |
# MAGIC | 5. Delta en pratique | `DESCRIBE HISTORY`, time travel, `RESTORE` |
# MAGIC | 6. RGPD | `DELETE` + `VACUUM`, et propagation de la suppression dans les couches |
# MAGIC
# MAGIC **Pré-requis** : Databricks Free Edition (calcul serverless). Aucun Kafka : la source est simulée en Python.
# MAGIC
# MAGIC **Conseil** : exécutez cellule par cellule (Maj + Entrée) et lisez les résultats avant de continuer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json
import random
from datetime import datetime, timedelta

CATALOG = "workspace"   # catalogue par défaut de la Free Edition. À adapter si le vôtre a un autre nom.
RESET = True            # True : repart de zéro à chaque exécution complète du notebook

spark.sql(f"USE CATALOG {CATALOG}")
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

if RESET:
    for t in ["bronze.payment_events", "silver.payments", "silver.payments_quarantine",
              "gold.dim_merchant", "gold.dim_date", "gold.fact_payment",
              "gold.revenue_daily", "gold.feat_customer_90d"]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")

print(f"Catalogue : {CATALOG} | schémas : bronze, silver, gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Générer des événements de paiement fictifs
# MAGIC
# MAGIC Chaque événement est un message JSON, comme ceux que Debezium publierait dans Kafka depuis la table `outbox`.
# MAGIC Pour que l'exercice soit réaliste, on injecte volontairement :
# MAGIC - des **messages invalides** (montant négatif, `payment_id` manquant, devise inconnue, JSON tronqué) ;
# MAGIC - des **doublons** (re-livraison « au moins une fois » de Kafka), dans un second lot.

# COMMAND ----------

random.seed(42)

MERCHANTS = [
    ("mer_01", "Café du Marché", "FR", "restauration"),
    ("mer_02", "Librairie Lumière", "FR", "retail"),
    ("mer_03", "Bike&Co", "DE", "sport"),
    ("mer_04", "Tapas Sol", "ES", "restauration"),
    ("mer_05", "Moda Roma", "IT", "mode"),
    ("mer_06", "CloudNote SaaS", "US", "logiciel"),
    ("mer_07", "London Tickets", "GB", "billetterie"),
    ("mer_08", "Berlin Gadgets", "DE", "électronique"),
    ("mer_09", "Pyrénées Voyages", "FR", "voyage"),
    ("mer_10", "StreamBox", "US", "abonnement"),
]
CUSTOMERS = [f"cus_{i:04d}" for i in range(1, 201)]
BASE = datetime(2026, 10, 5, 12, 0, 0)


def make_event(i):
    """Un événement de paiement valide. Un événement = un paiement (grain de la table de faits)."""
    status = random.choices(["captured", "failed", "refunded"], weights=[85, 10, 5])[0]
    ts = BASE - timedelta(seconds=random.randint(0, 35 * 24 * 3600))
    return {
        "event_id": f"evt_{i:06d}",
        "event_type": f"payment.{status}",
        "occurred_at": ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{random.randint(0, 999):03d}Z",
        "payment_id": f"pay_{i:06d}",
        "merchant_id": random.choice(MERCHANTS)[0],
        "customer_id": random.choice(CUSTOMERS),
        "amount": max(100, int(random.lognormvariate(3.8, 1.0) * 100)),   # en centimes
        "currency": random.choices(["EUR", "USD", "GBP"], weights=[80, 15, 5])[0],
        "payment_method": random.choice(["card", "card", "card", "sepa", "wallet"]),
        "ip_country": random.choice(["FR", "FR", "DE", "ES", "IT", "US", "GB"]),
        "device_type": random.choice(["mobile", "desktop"]),
        "fraud_score": round(random.betavariate(1, 12), 3),
    }


def to_json(ev):
    return json.dumps(ev, separators=(",", ":"), ensure_ascii=False)


def make_payload(i, corrupt_rate=0.03):
    """Retourne le JSON d'un événement, parfois volontairement corrompu."""
    ev = make_event(i)
    if random.random() < corrupt_rate:
        kind = random.choice(["montant_negatif", "payment_id_null", "devise_inconnue", "json_tronque"])
        if kind == "montant_negatif":
            ev["amount"] = -ev["amount"]
        elif kind == "payment_id_null":
            ev["payment_id"] = None
        elif kind == "devise_inconnue":
            ev["currency"] = "XXX"
        else:
            s = to_json(ev)
            return s[: len(s) // 2]
    return to_json(ev)


# Lot A : 1500 nouveaux événements. Lot B : 150 re-livraisons du lot A + 500 nouveaux événements.
batch_a = [make_payload(i) for i in range(1, 1501)]
batch_b = random.sample(batch_a, 150) + [make_payload(i) for i in range(1501, 2001)]
random.shuffle(batch_b)

print(f"Lot A : {len(batch_a)} messages | Lot B : {len(batch_b)} messages (dont 150 re-livrés)")
print("Exemple de message :", batch_a[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze : « tel que reçu »
# MAGIC
# MAGIC Règles de la couche bronze :
# MAGIC - on stocke le **message brut** (colonne `payload`) + les métadonnées Kafka ;
# MAGIC - **ajout seul** (`append`), aucune correction, aucun rejet ;
# MAGIC - c'est la source **rejouable** : si silver est faux, on le recalcule depuis bronze.

# COMMAND ----------

_offset = {"n": 0}


def append_to_bronze(payloads):
    rows = []
    for p in payloads:
        rows.append((p, "stripe.outbox.payments", _offset["n"] % 3, _offset["n"]))
        _offset["n"] += 1
    df = (spark.createDataFrame(
              rows, "payload STRING, topic STRING, kafka_partition INT, kafka_offset BIGINT")
          .withColumn("ingested_at", F.current_timestamp()))
    df.write.format("delta").mode("append").saveAsTable("bronze.payment_events")


append_to_bronze(batch_a)
print("Lignes en bronze :", spark.table("bronze.payment_events").count())
display(spark.table("bronze.payment_events").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver : « propre et fiable »
# MAGIC
# MAGIC On **parse** le JSON, on **valide**, on envoie les rejets en **quarantaine** (jamais à la poubelle), et on **dédoublonne**.
# MAGIC
# MAGIC Le point clé : l'insertion se fait par `MERGE ... WHEN NOT MATCHED THEN INSERT`. Rejouer le traitement
# MAGIC 1, 2 ou 10 fois donne le **même résultat** : c'est l'**idempotence** des consommateurs, garantie qu'on a promise
# MAGIC avec l'outbox (livraison « au moins une fois »).

# COMMAND ----------

EVENT_SCHEMA = (
    "event_id STRING, event_type STRING, occurred_at STRING, payment_id STRING, "
    "merchant_id STRING, customer_id STRING, amount BIGINT, currency STRING, "
    "payment_method STRING, ip_country STRING, device_type STRING, fraud_score DOUBLE"
)


def parse_and_flag(df_bronze):
    """Parse le JSON et ajoute `reject_reason` (NULL = événement valide)."""
    parsed = (df_bronze
              .withColumn("e", F.from_json("payload", EVENT_SCHEMA))
              .select("payload", "ingested_at", "kafka_offset", "e.*"))
    reason = (
        F.when(F.col("event_id").isNull(), "json_invalide_ou_event_id_manquant")
         .when(F.col("payment_id").isNull(), "payment_id_manquant")
         .when(F.col("amount").isNull() | (F.col("amount") <= 0), "montant_invalide")
         .when(F.col("currency").isNull() | ~F.col("currency").isin("EUR", "USD", "GBP"), "devise_inconnue")
    )
    return parsed.withColumn("reject_reason", reason)


def dedup_valid(flagged):
    """Garde les événements valides, une seule ligne par event_id (la plus récente)."""
    w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc(), F.col("kafka_offset").desc())
    return (flagged.filter("reject_reason IS NULL")
            .withColumn("rn", F.row_number().over(w)).filter("rn = 1")
            .select(
                "event_id", "event_type",
                F.to_timestamp("occurred_at").alias("occurred_at"),
                "payment_id", "merchant_id", "customer_id",
                F.col("amount").alias("amount_minor"),
                (F.col("amount") / 100.0).alias("amount_major"),
                "currency",
                F.split("event_type", r"\.").getItem(1).alias("status"),
                "payment_method", "ip_country", "device_type", "fraud_score", "ingested_at"))


def process_silver():
    """Traitement complet bronze -> silver (idempotent : rejouable sans doublon)."""
    flagged = parse_and_flag(spark.table("bronze.payment_events"))

    # Quarantaine : on garde les rejets et leur motif, pour analyse et correction à la source.
    (flagged.filter("reject_reason IS NOT NULL")
        .select("payload", "reject_reason", "ingested_at").dropDuplicates(["payload"])
        .write.format("delta").mode("overwrite").saveAsTable("silver.payments_quarantine"))

    valid = dedup_valid(flagged)
    valid.limit(0).write.format("delta").mode("ignore").saveAsTable("silver.payments")  # crée la table si absente
    valid.createOrReplaceTempView("v_valid")
    return spark.sql("""
        MERGE INTO silver.payments AS t
        USING v_valid AS s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)


def silver_report():
    n_b = spark.table("bronze.payment_events").count()
    n_s = spark.table("silver.payments").count()
    n_d = spark.table("silver.payments").select("event_id").distinct().count()
    n_q = spark.table("silver.payments_quarantine").count()
    print(f"bronze={n_b} | silver={n_s} (event_id distincts={n_d}) | quarantaine={n_q}")
    assert n_s == n_d, "Doublons en silver !"


display(process_silver())
silver_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Voir les rejets (quarantaine)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT reject_reason, COUNT(*) AS nb
# MAGIC FROM silver.payments_quarantine
# MAGIC GROUP BY reject_reason
# MAGIC ORDER BY nb DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test d'idempotence : un second lot (avec re-livraisons), puis on rejoue plusieurs fois
# MAGIC
# MAGIC **À observer** : bronze grossit de 650 lignes (tout est conservé, doublons compris),
# MAGIC mais silver n'ajoute **que les événements nouveaux** et valides. Rejouer ne change plus rien
# MAGIC (`num_inserted_rows = 0` aux exécutions suivantes).

# COMMAND ----------

append_to_bronze(batch_b)
print("--- Après le lot B ---")
display(process_silver())
silver_report()

print("--- On rejoue le même traitement (doit insérer 0 ligne) ---")
display(process_silver())
silver_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold : « prêt pour l'usage »
# MAGIC
# MAGIC Trois sortes de tables gold :
# MAGIC 1. le **schéma en étoile** : `fact_payment` (grain : **un paiement**), `dim_merchant`, `dim_date` ;
# MAGIC 2. une table **agrégée** pour les dashboards : `revenue_daily` ;
# MAGIC 3. une table de **features** pour le ML : `feat_customer_90d` (c'est elle que le flux 6 publierait dans Redis).
# MAGIC
# MAGIC On met le calcul dans une fonction `build_gold()` pour pouvoir le **rejouer** plus tard (étape RGPD).

# COMMAND ----------

def build_gold():
    # Dimension marchand (surrogate key = merchant_key)
    dim_m = spark.createDataFrame(
        [(i + 1, m[0], m[1], m[2], m[3]) for i, m in enumerate(MERCHANTS)],
        "merchant_key INT, merchant_id STRING, merchant_name STRING, country STRING, category STRING")
    dim_m.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
         .saveAsTable("gold.dim_merchant")

    # Dimension date
    spark.sql("""
        CREATE OR REPLACE TABLE gold.dim_date AS
        SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
               d AS full_date,
               year(d) AS year,
               month(d) AS month,
               date_format(d, 'yyyy-MM') AS year_month,
               date_format(d, 'EEEE') AS day_name
        FROM (SELECT explode(sequence(DATE'2026-08-01', DATE'2026-10-31', INTERVAL 1 DAY)) AS d)
    """)

    # Table de faits : un paiement par ligne
    spark.sql("""
        CREATE OR REPLACE TABLE gold.fact_payment AS
        SELECT s.payment_id,
               CAST(date_format(s.occurred_at, 'yyyyMMdd') AS INT) AS date_key,
               m.merchant_key,
               s.customer_id,
               s.amount_major, s.currency, s.status,
               s.payment_method, s.device_type, s.ip_country, s.fraud_score,
               s.occurred_at
        FROM silver.payments s
        JOIN gold.dim_merchant m ON s.merchant_id = m.merchant_id
    """)

    # Agrégat pour dashboards
    spark.sql("""
        CREATE OR REPLACE TABLE gold.revenue_daily AS
        SELECT d.full_date, m.merchant_id, f.currency,
               SUM(f.amount_major) AS revenue, COUNT(*) AS nb_payments
        FROM gold.fact_payment f
        JOIN gold.dim_date d ON f.date_key = d.date_key
        JOIN gold.dim_merchant m ON f.merchant_key = m.merchant_key
        WHERE f.status = 'captured'
        GROUP BY d.full_date, m.merchant_id, f.currency
    """)

    # Features "lentes" (fenêtre de 90 jours) pour le scoring
    spark.sql("""
        CREATE OR REPLACE TABLE gold.feat_customer_90d AS
        SELECT customer_id,
               COUNT(*) AS nb_payments_90d,
               ROUND(AVG(amount_major), 2) AS avg_amount_90d,
               ROUND(AVG(fraud_score), 4) AS avg_fraud_score_90d
        FROM silver.payments
        WHERE status = 'captured'
          AND occurred_at >= (SELECT MAX(occurred_at) FROM silver.payments) - INTERVAL 90 DAYS
        GROUP BY customer_id
    """)


build_gold()
for t in ["gold.dim_merchant", "gold.dim_date", "gold.fact_payment", "gold.revenue_daily", "gold.feat_customer_90d"]:
    print(f"{t}: {spark.table(t).count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Requêtes analytiques (SQL standard) sur le schéma en étoile
# MAGIC
# MAGIC **Question métier 1** : quel est le chiffre d'affaires par mois, pays de marchand et devise ?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.year_month, m.country, f.currency,
# MAGIC        ROUND(SUM(f.amount_major), 2) AS revenue,
# MAGIC        COUNT(*) AS nb_payments
# MAGIC FROM gold.fact_payment f
# MAGIC JOIN gold.dim_date d     ON f.date_key = d.date_key
# MAGIC JOIN gold.dim_merchant m ON f.merchant_key = m.merchant_key
# MAGIC WHERE f.status = 'captured'
# MAGIC GROUP BY d.year_month, m.country, f.currency
# MAGIC ORDER BY d.year_month, revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Question métier 2** : quels marchands ont le taux de remboursement le plus élevé ?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT m.merchant_name,
# MAGIC        COUNT(*) AS nb_payments,
# MAGIC        ROUND(100.0 * SUM(CASE WHEN f.status = 'refunded' THEN 1 ELSE 0 END) / COUNT(*), 1) AS refund_rate_pct
# MAGIC FROM gold.fact_payment f
# MAGIC JOIN gold.dim_merchant m ON f.merchant_key = m.merchant_key
# MAGIC GROUP BY m.merchant_name
# MAGIC ORDER BY refund_rate_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Question métier 3** : le score de fraude moyen diffère-t-il selon le statut du paiement et l'appareil ?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, device_type,
# MAGIC        ROUND(AVG(fraud_score), 4) AS avg_fraud_score,
# MAGIC        COUNT(*) AS nb
# MAGIC FROM gold.fact_payment
# MAGIC GROUP BY status, device_type
# MAGIC ORDER BY status, device_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delta en pratique : journal, time travel, restauration
# MAGIC
# MAGIC Ces commandes sont **propres à Delta / Databricks** (voir notre discussion sur le SQL standard).
# MAGIC `DESCRIBE HISTORY` lit le journal `_delta_log` : c'est un **journal d'audit** intégré à la table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.payments

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL silver.payments

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scénario « job défectueux » : suppression par erreur, puis `RESTORE`
# MAGIC
# MAGIC Un job mal écrit supprime toutes les lignes d'un marchand. Grâce au journal, on revient en arrière.

# COMMAND ----------

v_before = spark.sql("DESCRIBE HISTORY silver.payments").agg(F.max("version")).first()[0]
n_before = spark.table("silver.payments").count()

spark.sql("DELETE FROM silver.payments WHERE merchant_id = 'mer_03'")   # l'erreur
n_after = spark.table("silver.payments").count()
print(f"Version avant l'erreur : {v_before}")
print(f"Lignes avant = {n_before} | après l'erreur = {n_after}")

# Time travel : on peut relire l'ancien état sans rien restaurer
n_tt = spark.sql(f"SELECT COUNT(*) AS n FROM silver.payments VERSION AS OF {v_before}").first()["n"]
print(f"Lecture time travel (VERSION AS OF {v_before}) = {n_tt} lignes")

# Restauration
spark.sql(f"RESTORE TABLE silver.payments TO VERSION AS OF {v_before}")
n_restored = spark.table("silver.payments").count()
print(f"Après RESTORE = {n_restored} lignes")
assert n_restored == n_before

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Le RESTORE est lui-même enregistré dans l'historique : rien n'est effacé, tout est traçable.
# MAGIC DESCRIBE HISTORY silver.payments

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. RGPD : droit à l'effacement, `DELETE` + `VACUUM`
# MAGIC
# MAGIC Trois points à retenir, qui sont des questions classiques de jury :
# MAGIC
# MAGIC 1. **`DELETE` est logique.** Les anciens fichiers restent lisibles par time travel tant que `VACUUM` ne les a pas supprimés (rétention par défaut : 7 jours).
# MAGIC 2. **Il faut supprimer dans toutes les couches.** Bronze contient le message brut, donc les données personnelles : si on ne supprime qu'en silver, la donnée reste en bronze. Et gold doit être reconstruit.
# MAGIC 3. **Pour des paiements, la loi impose souvent de conserver** certaines données (obligations comptables, lutte contre le blanchiment). En vrai, on pseudonymise ou on anonymise ce qui n'a plus à être conservé, et on documente les durées de conservation.

# COMMAND ----------

# Choix du client à effacer : celui qui a le plus de paiements
target = (spark.sql("SELECT customer_id FROM silver.payments GROUP BY customer_id ORDER BY COUNT(*) DESC LIMIT 1")
          .first()["customer_id"])
like = f'%"customer_id":"{target}"%'


def counts():
    return {
        "bronze": spark.sql(f"SELECT COUNT(*) AS n FROM bronze.payment_events WHERE payload LIKE '{like}'").first()["n"],
        "silver": spark.sql(f"SELECT COUNT(*) AS n FROM silver.payments WHERE customer_id = '{target}'").first()["n"],
        "gold.fact_payment": spark.sql(f"SELECT COUNT(*) AS n FROM gold.fact_payment WHERE customer_id = '{target}'").first()["n"],
        "gold.feat_customer_90d": spark.sql(f"SELECT COUNT(*) AS n FROM gold.feat_customer_90d WHERE customer_id = '{target}'").first()["n"],
    }


print("Client à effacer :", target)
print("Avant :", counts())

# COMMAND ----------

v_pre_rgpd = spark.sql("DESCRIBE HISTORY silver.payments").agg(F.max("version")).first()[0]

# 1) On efface à la source (bronze) ET en silver
spark.sql(f"DELETE FROM bronze.payment_events WHERE payload LIKE '{like}'")
spark.sql(f"DELETE FROM silver.payments WHERE customer_id = '{target}'")
print("Après DELETE bronze + silver :", counts())

# 2) Gold est dérivé : on le reconstruit
build_gold()
print("Après reconstruction de gold :", counts())

# 3) Rejouer silver depuis bronze ne ressuscite pas le client (bronze a été nettoyé)
process_silver()
print("Après rejeu de silver        :", counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Le piège : le time travel peut encore relire la donnée effacée

# COMMAND ----------

n_old = spark.sql(
    f"SELECT COUNT(*) AS n FROM silver.payments VERSION AS OF {v_pre_rgpd} WHERE customer_id = '{target}'"
).first()["n"]
print(f"Lignes de {target} encore lisibles en time travel (version {v_pre_rgpd}) : {n_old}")

# COMMAND ----------

# MAGIC %md
# MAGIC `VACUUM` supprime physiquement les fichiers qui ne sont plus référencés par la version courante **et** plus anciens que la rétention.
# MAGIC Avec la rétention par défaut (7 jours), la commande ci-dessous **ne supprimera donc rien d'immédiat** : c'est voulu, c'est une protection.

# COMMAND ----------

display(spark.sql("VACUUM silver.payments DRY RUN"))   # DRY RUN : liste ce qui serait supprimé, sans rien supprimer

# COMMAND ----------

# MAGIC %md
# MAGIC Pour forcer la suppression immédiate (**jamais en production sans y réfléchir** : on perd le time travel et on peut
# MAGIC casser des lectures en cours), il faut ramener la rétention à zéro. Cette option peut être refusée par le calcul
# MAGIC serverless de la Free Edition ; ce n'est pas bloquant pour l'exercice.
# MAGIC
# MAGIC ```sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM silver.payments RETAIN 0 HOURS;
# MAGIC ```
# MAGIC
# MAGIC Dans une vraie architecture RGPD, on **planifie** `VACUUM` avec une rétention alignée sur la politique de conservation,
# MAGIC et on **documente** le délai effectif entre la demande d'effacement et la suppression physique.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Compactage : `OPTIMIZE`
# MAGIC
# MAGIC Beaucoup de petites écritures (streaming, micro-batch) produisent beaucoup de petits fichiers. `OPTIMIZE` les regroupe
# MAGIC pour accélérer les lectures. Comparez `numFiles` avant et après.

# COMMAND ----------

before = spark.sql("DESCRIBE DETAIL silver.payments").first()["numFiles"]
display(spark.sql("OPTIMIZE silver.payments"))
after = spark.sql("DESCRIBE DETAIL silver.payments").first()["numFiles"]
print(f"Fichiers : {before} -> {after}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. À explorer ensuite (dans l'interface)
# MAGIC
# MAGIC 1. **Catalog Explorer** : ouvrez `workspace > silver > payments`, puis l'onglet **Lineage** : vous voyez bronze, silver et gold reliés. C'est la traçabilité que Unity Catalog apporte pour la conformité.
# MAGIC 2. **Onglet History** de la table : c'est l'équivalent graphique de `DESCRIBE HISTORY`.
# MAGIC 3. **Exercice schema enforcement** : essayez d'insérer en silver une ligne avec un type incorrect (par exemple du texte dans `amount_minor`) et lisez l'erreur : Delta refuse au lieu de polluer la table.
# MAGIC 4. **Exercice schema evolution** : ajoutez une colonne `risk_level` au lot suivant et utilisez `ALTER TABLE ... ADD COLUMN`.
# MAGIC 5. **Exercice masquage** : pseudonymisez `customer_id` avec `sha2(customer_id, 256)` dans silver, et voyez ce que cela change pour l'effacement (le lien reste possible tant qu'on sait recalculer le hash).
# MAGIC 6. **Suite du cursus** : Unity Catalog (permissions par colonne, masquage dynamique), puis MLflow et le Feature Store, qui lisent la couche gold.
# MAGIC
# MAGIC **Nettoyage** : remettre `RESET = True` et relancer la cellule 0 supprime toutes les tables de l'exercice.
