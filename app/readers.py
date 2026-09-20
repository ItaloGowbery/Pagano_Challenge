import logging
from datetime import datetime, timezone

from asyncua import Client, ua

from .core import DataPack, Reader

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
