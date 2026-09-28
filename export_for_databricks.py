"""Junta os CSVs gravados no MinIO num único arquivo, para subir ao Databricks.

Rodar com o venv ativo e o MinIO no ar:
    python export_for_databricks.py
"""

import csv
import io

import boto3

ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET = "telemetry"
PREFIX = "readings/"
OUTPUT = "readings_export.csv"

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

paginator = s3.get_paginator("list_objects_v2")
keys = [
    obj["Key"]
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX)
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".csv")
]
keys.sort()
print(f"{len(keys)} arquivo(s) encontrado(s)")

total = 0
with open(OUTPUT, "w", newline="") as out:
    writer = csv.writer(out)
    writer.writerow(["tag", "value", "status", "timestamp"])

    for key in keys:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()
        reader = csv.reader(io.StringIO(body))
        next(reader, None)  # pula o cabeçalho de cada arquivo
        for row in reader:
            writer.writerow(row)
            total += 1

print(f"{total} linha(s) gravada(s) em {OUTPUT}")
