# Pagano_Challenge - Industrial Data Pipeline

Ingestion pipeline for industrial telemetry. It reads sensor tags from an
OPC-UA server, publishes each reading to a Kafka topic and persists it in
Postgres, with the whole infrastructure declared in Docker Compose.

Kafka sits between producer and consumer so neither blocks the other: if the
database goes down, readings keep accumulating in the topic and the sink
resumes from its last offset once it comes back. The same stream can feed
additional destinations without touching the code that reads from the plant.

The data source is a simulated OPC-UA server, standing in for real equipment
during development.

![Sem título-2024-07-12-2230](https://github.com/user-attachments/assets/d8bb049f-3513-48b3-9d97-c75b1a536e64)
