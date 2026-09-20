import asyncio
import logging

from .core import Operation, Scheduler
from .readers import KafkaReader, OpcuaReader
from .settings import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC, OPCUA_TAGS, OPCUA_URL,
    POSTGRES_DSN, READ_INTERVAL, SINK_INTERVAL,
)
from .writers import KafkaWriter, PostgresWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("asyncua").setLevel(logging.WARNING)
log = logging.getLogger("pipeline")


def build_operations() -> list[Operation]:
    return [
        Operation(
            name="opcua->kafka",
            reader=OpcuaReader(url=OPCUA_URL, tags=OPCUA_TAGS),
            writer=KafkaWriter(bootstrap=KAFKA_BOOTSTRAP, topic=KAFKA_TOPIC),
            interval=READ_INTERVAL,
        ),
        Operation(
            name="kafka->postgres",
            reader=KafkaReader(bootstrap=KAFKA_BOOTSTRAP, topic=KAFKA_TOPIC, group_id="postgres-sink"),
            writer=PostgresWriter(dsn=POSTGRES_DSN),
            interval=SINK_INTERVAL,
        ),
    ]


async def main() -> None:
    scheduler = Scheduler(build_operations())
    log.info("pipeline iniciando com %d operation(s)", len(scheduler.operations))
    await scheduler.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("pipeline encerrado")
