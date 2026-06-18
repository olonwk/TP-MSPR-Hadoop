# Infrastructure locale — FinData / DEPE855

Environnement Docker pour tester le pipeline localement.

## Prérequis

1. Docker Desktop installé et lancé
2. Télécharger le driver JDBC PostgreSQL :
   ```
   mkdir drivers
   curl -L https://jdbc.postgresql.org/download/postgresql-42.7.0.jar -o drivers/postgresql-42.7.0.jar
   ```

## Démarrage

```bash
docker compose up -d
```

| Service | URL | Credentials |
|---------|-----|-------------|
| NiFi UI | https://localhost:8443/nifi | admin / Admin12345678 |
| Kafka UI | http://localhost:8080 | — |
| PostgreSQL | localhost:5432 | admin / admin123 / bankdb |

## Création du topic Kafka

```bash
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic findata.logs.raw \
  --partitions 4 --replication-factor 1
```

## Arrêt

```bash
docker compose down        # conserve les volumes
docker compose down -v     # supprime les volumes (reset complet)
```
