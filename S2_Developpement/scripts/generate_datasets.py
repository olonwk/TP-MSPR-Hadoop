"""
generate_datasets.py
Génère les jeux de données synthétiques pour les 3 flux FinData Solutions.
Usage: python generate_datasets.py [--date AAAA-MM-JJ] [--double]
"""

import argparse
import csv
import json
import os
import random
import hashlib
from datetime import datetime, timedelta

try:
    from faker import Faker
    FAKER_AVAILABLE = True
except ImportError:
    FAKER_AVAILABLE = False
    print("Faker non installé. Génération sans noms réalistes. pip install faker")

random.seed(42)

BANQUES = ["BNP", "SG", "CA", "LCL", "CM", "HSBC", "BPOP", "BPCE", "AXA", "ING",
           "BRED", "CIC", "HELLO", "FORTUNEO", "REVOLUT"]
DEVISES = ["EUR", "USD", "GBP", "JPY", "CHF", "CAD", "AUD", "SEK", "NOK", "DKK",
           "PLN", "CZK", "HUF", "RON", "BGN"]
STATUTS = ["OK"] * 90 + ["FRAUD"] * 7 + ["PENDING"] * 2 + [None]
SOURCES_LEGALES = ["ECB", "Bloomberg", "Reuters"]
SOURCES_TOUTES = SOURCES_LEGALES + ["FAKE_DATA", "UNKNOWN_FEED", "INTERNAL"]
SERVICES = ["auth-service", "payment-service", "fraud-detector", "api-gateway",
            "notif-service", "reporting-engine", "kyc-service"]
LEVELS = ["DEBUG"] * 30 + ["INFO"] * 45 + ["WARNING"] * 12 + ["ERROR"] * 10 + ["CRITICAL"] * 3
ERROR_CODES = [f"ERR-{str(i).zfill(4)}" for i in [1001, 1002, 1003, 1004, 2001, 2002,
                                                     3001, 3002, 3003, 4001, 5001, 5002]]
TAUX_BASE = {
    "EUR": 1.0, "USD": 1.08, "GBP": 0.86, "JPY": 162.5, "CHF": 0.97,
    "CAD": 1.47, "AUD": 1.65, "SEK": 11.4, "NOK": 11.6, "DKK": 7.46,
    "PLN": 4.32, "CZK": 25.1, "HUF": 395.0, "RON": 4.97, "BGN": 1.96
}


def gen_iban():
    return "FR76" + "".join([str(random.randint(0, 9)) for _ in range(23)])


def gen_client_id():
    return random.randint(1000, 999999)


def generate_transactions(date_str, n=550000):
    print(f"Génération de {n} transactions pour {date_str}...")
    date = datetime.strptime(date_str, "%Y-%m-%d")
    rows = []
    for i in range(1, n + 1):
        montant = round(random.uniform(-500, 50000), 2) if random.random() < 0.01 else round(random.uniform(0.01, 50000), 2)
        delta = timedelta(seconds=random.randint(0, 86399))
        rows.append({
            "transaction_id": f"TXN-{date_str.replace('-','')}-{str(i).zfill(8)}",
            "client_id": gen_client_id(),
            "iban": gen_iban(),
            "montant": montant,
            "devise": random.choice(DEVISES),
            "date_heure": (date + delta).strftime("%Y-%m-%d %H:%M:%S"),
            "code_banque": random.choice(BANQUES),
            "statut": random.choice(STATUTS)
        })
        if i % 100000 == 0:
            print(f"  {i}/{n} transactions générées")
    return rows


def generate_courses(date_str):
    print(f"Génération des cours boursiers pour {date_str}...")
    date = datetime.strptime(date_str, "%Y-%m-%d")
    rows = []
    for devise, taux_base in TAUX_BASE.items():
        if devise == "EUR":
            continue
        nb_ticks = random.randint(8, 25)
        for j in range(nb_ticks):
            heure = timedelta(hours=8 + j * (10 / nb_ticks), minutes=random.randint(0, 59))
            taux = round(taux_base * random.uniform(0.998, 1.002), 6)
            source = random.choice(SOURCES_TOUTES) if random.random() < 0.15 else random.choice(SOURCES_LEGALES)
            rows.append({
                "devise": devise,
                "taux_eur": taux,
                "horodatage": (date + heure).strftime("%Y-%m-%d %H:%M:%S"),
                "source": source
            })
    return rows


def generate_logs(date_str, n=220000):
    print(f"Génération de {n} logs applicatifs pour {date_str}...")
    date = datetime.strptime(date_str, "%Y-%m-%d")
    rows = []
    for i in range(n):
        level = random.choice(LEVELS)
        service = random.choice(SERVICES)
        delta = timedelta(seconds=random.randint(0, 86399))
        ts = (date + delta).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        if level in ("ERROR", "CRITICAL") and random.random() < 0.7:
            err_code = random.choice(ERROR_CODES)
            message = f"Exception raised: {err_code} - {random.choice(['Timeout', 'NullPointer', 'ConnectionRefused', 'AuthFailed', 'InvalidIBAN'])}"
        else:
            message = f"Operation completed in {random.randint(1, 5000)}ms"
        entry = {
            "timestamp": ts,
            "service": service,
            "level": level,
            "message": message,
        }
        if random.random() < 0.7:
            entry["user_id"] = str(random.randint(1000, 999999))
        rows.append(entry)
    return rows


def write_csv(rows, filepath, fieldnames):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  -> Écrit : {filepath} ({len(rows)} lignes)")


def write_json(rows, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"  -> Écrit : {filepath} ({len(rows)} lignes JSONL)")


def main():
    parser = argparse.ArgumentParser(description="Génération des datasets FinData")
    parser.add_argument("--date", default="2024-01-15", help="Date de traitement AAAA-MM-JJ")
    parser.add_argument("--double", action="store_true", help="Doubler le volume (simulation montée en charge S3)")
    args = parser.parse_args()

    multiplier = 2 if args.double else 1
    base_dir = os.path.join(os.path.dirname(__file__), "..", "test_data")

    transactions = generate_transactions(args.date, n=550000 * multiplier)
    write_csv(
        transactions,
        os.path.join(base_dir, f"transactions_{args.date}.csv"),
        fieldnames=["transaction_id", "client_id", "iban", "montant", "devise", "date_heure", "code_banque", "statut"]
    )

    courses = generate_courses(args.date)
    write_csv(
        courses,
        os.path.join(base_dir, f"courses_{args.date}.csv"),
        fieldnames=["devise", "taux_eur", "horodatage", "source"]
    )

    logs = generate_logs(args.date, n=220000 * multiplier)
    write_json(logs, os.path.join(base_dir, f"logs_{args.date}.json"))

    print("\nDatasets générés avec succès.")
    print(f"Chargement HDFS (à exécuter sur le cluster) :")
    d = args.date
    print(f"  hdfs dfs -mkdir -p /raw/transactions/ /raw/courses/ /staging/logs/ /ref/cours/ /production/transactions/ /production/logs/")
    print(f"  hdfs dfs -put test_data/transactions_{d}.csv /raw/transactions/")
    print(f"  hdfs dfs -put test_data/courses_{d}.csv /raw/courses/{d}.csv")
    print(f"  hdfs dfs -put test_data/logs_{d}.json /staging/logs/")


if __name__ == "__main__":
    main()
