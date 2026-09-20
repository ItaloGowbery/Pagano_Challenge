import json
import logging

from aiokafka import AIOKafkaProducer

from .core import DataPack, Writer

import asyncpg

log = logging.getLogger(__name__)


class KafkaWriter(Writer):
    """Publica o lote inteiro como uma mensagem JSON."""

    def __init__(self, bootstrap: str, topic: str) -> None:
        self.bootstrap = bootstrap
        self.topic = topic
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        await self._producer.start()
        log.info("produzindo para o tópico '%s'", self.topic)

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None

    async def write(self, packs: list[DataPack]) -> None:
        if self._producer is None:
            await self.start()
        payload = [p.to_dict() for p in packs]
        await self._producer.send_and_wait(self.topic, payload)

class PostgresWriter(Writer):
    """Grava as leituras na tabela readings."""

    INSERT = """
        INSERT INTO readings (tag, value, status, timestamp)
        VALUES ($1, $2, $3, $4)
    """

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def start(self) -> None:
        self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=5)
        log.info("pool de conexões do Postgres pronto")

    async def stop(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def write(self, packs: list[DataPack]) -> None:
        if self._pool is None:
            await self.start()
        rows = [(p.tag, p.value, p.status, p.timestamp) for p in packs]
        async with self._pool.acquire() as conn:
            await conn.executemany(self.INSERT, rows)
