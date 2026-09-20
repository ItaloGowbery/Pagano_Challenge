import json
import logging

from aiokafka import AIOKafkaProducer

from .core import DataPack, Writer

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
