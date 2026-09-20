import os

OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4840/freeopcua/server/")
OPCUA_TAGS = ["ns=2;i=2", "ns=2;i=3", "ns=2;i=4", "ns=2;i=5"]

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "opcua.readings")

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN", "postgresql://pipeline:pipeline@localhost:5432/telemetry"
)

READ_INTERVAL = float(os.getenv("READ_INTERVAL", "5"))
SINK_INTERVAL = float(os.getenv("SINK_INTERVAL", "5"))
