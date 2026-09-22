# Industrial Data Pipeline

Reads sensor tags from an OPC-UA server, publishes them to Kafka and stores
them in Postgres. Everything runs in Docker.

```
OPC-UA ──▶ operation 1 ──▶ Kafka ──▶ operation 2 ──▶ Postgres
```

## Goal

I build instrumentation but had never worked with the tools used to move data
at scale, so I built a pipeline that starts where I already know the ground —
an industrial protocol — and ends in a database. The point was to learn Kafka,
Docker and the shape of an ingestion pipeline by writing one.

The OPC-UA server is simulated, since I don't have a plant at home.

## Running it

```bash
docker compose up -d                  # Kafka and Postgres

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m app.simulator               # terminal 1
python -m app.main                    # terminal 2
```

Check the data:

```bash
docker exec -it postgres psql -U pipeline -d telemetry \
  -c "SELECT * FROM readings ORDER BY id DESC LIMIT 10;"
```

## How it works

`app/core.py` holds four pieces and no transport code at all:

- **DataPack** — one reading: tag, value, quality, timestamp
- **Reader** / **Writer** — contracts for where data comes from and goes to
- **Operation** — binds a reader to a writer and an interval
- **Scheduler** — runs the operations, isolating failures

The concrete classes live in `readers.py` and `writers.py`. Adding a
destination means writing one new writer and one line in `main.py`.

## What I learned

**Kafka isn't a queue, it's a log.** Messages stay after being read, and each
consumer group tracks its own position. That's what lets several destinations
consume the same stream independently.

**Offsets should be committed after the write, not before.** With auto-commit
on, a failed database write would mark the messages as processed and lose them.
Committing manually after a successful write gives at-least-once delivery.

**Staying up is a design decision.** Wrapping each cycle in try/except is what
separates a script that dies overnight from a service that recovers.

**Testing failure finds bugs that testing success doesn't.** Killing the
OPC-UA server showed the client object survived the dead session, so every
later read failed against a dead handle. It only surfaced because I killed
the server on purpose.

**Two timestamps beat one.** Recording both when the sensor measured and when
the row landed makes the pipeline's own latency a thing you can query.

## Resilience

Each dependency was killed mid-run and brought back:

| Failure | Result |
|---|---|
| Kafka down | Both operations log and retry; process survives |
| Postgres down | Producer keeps publishing, backlog drains on recovery, nothing lost |
| Simulator down | Reconnects — after fixing the bug above |

Known limitation: if Kafka is unreachable at startup, the process exits
instead of retrying. Recovery only works once the loops are running.


