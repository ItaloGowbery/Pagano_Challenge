import logging
from datetime import datetime, timezone

from asyncua import Client, ua

from .core import DataPack, Reader

import json

from aiokafka import AIOKafkaConsumer

log = logging.getLogger(__name__)


class OpcuaReader(Reader):
    """Lê um conjunto de tags de um servidor OPC-UA."""

    def __init__(self, url: str, tags: list[str]) -> None:
        self.url = url
        self.tags = tags
        self._client: Client | None = None

    async def start(self) -> None:
        self._client = Client(url=self.url)
        await self._client.connect()
        log.info("conectado ao OPC-UA em %s", self.url)

    async def stop(self) -> None:
        if self._client:
            await self._client.disconnect()
            self._client = None

    async def read(self) -> list[DataPack]:
        if self._client is None:
            await self.start()

        packs: list[DataPack] = []
        for tag in self.tags:
            node = self._client.get_node(tag)
            dv = await node.read_data_value()
            packs.append(
                DataPack(
                    tag=tag,
                    value=float(dv.Value.Value) if dv.Value.Value is not None else None,
                    status="good" if dv.StatusCode.is_good() else "bad",
                    timestamp=dv.SourceTimestamp or datetime.now(timezone.utc),
                )
            )
        return packs

class KafkaReader(Reader):
    """Consome um tópico e devolve o que chegou desde a última leitura."""

    def __init__(self, bootstrap: str, topic: str, group_id: str, max_records: int = 500) -> None:
        self.bootstrap = bootstrap
        self.topic = topic
        self.group_id = group_id
        self.max_records = max_records
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda b: json.loads(b.decode()),
        )
        await self._consumer.start()
        log.info("consumindo '%s' como grupo '%s'", self.topic, self.group_id)

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

    async def read(self) -> list[DataPack]:
        if self._consumer is None:
            await self.start()

        batches = await self._consumer.getmany(timeout_ms=1000, max_records=self.max_records)
        packs: list[DataPack] = []
        for _tp, messages in batches.items():
            for msg in messages:
                for item in msg.value:
                    packs.append(DataPack.from_dict(item))
        return packs

    async def commit(self) -> None:
        if self._consumer:
            await self._consumer.commit()
