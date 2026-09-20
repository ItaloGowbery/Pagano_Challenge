import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

log = logging.getLogger(__name__)


@dataclass
class DataPack:
    """Uma leitura, no formato que atravessa todo o pipeline."""

    tag: str
    value: float | None
    status: str = "good"
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "DataPack":
        return cls(
            tag=d["tag"],
            value=d.get("value"),
            status=d.get("status", "good"),
            timestamp=datetime.fromisoformat(d["timestamp"]),
        )


class Reader(ABC):
    """De onde os dados vêm."""

    async def start(self) -> None:
        """Abre conexões. Chamado uma vez, antes da primeira leitura."""

    async def stop(self) -> None:
        """Fecha conexões."""

    @abstractmethod
    async def read(self) -> list[DataPack]:
        """Devolve o que houver para ler agora. Pode vir lista vazia."""


class Writer(ABC):
    """Para onde os dados vão."""

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    @abstractmethod
    async def write(self, packs: list[DataPack]) -> None:
        ...


@dataclass
class Operation:
    """Liga um Reader a um Writer e diz de quanto em quanto tempo isso roda."""

    name: str
    reader: Reader
    writer: Writer
    interval: float = 10.0
    active: bool = True

    async def start(self) -> None:
        await self.reader.start()
        await self.writer.start()

    async def stop(self) -> None:
        await self.reader.stop()
        await self.writer.stop()

    async def execute(self) -> None:
        packs = await self.reader.read()
        if not packs:
            log.debug("[%s] nada para ler", self.name)
            return
        await self.writer.write(packs)
        log.info("[%s] %d registro(s) movido(s)", self.name, len(packs))


class Scheduler:
    """Roda cada Operation no seu próprio intervalo, em paralelo.

    Um erro dentro de uma operation não derruba as outras nem para o loop:
    loga e tenta de novo no próximo ciclo.
    """

    def __init__(self, operations: list[Operation]) -> None:
        self.operations = [op for op in operations if op.active]
        self._tasks: list[asyncio.Task] = []

    async def _loop(self, op: Operation) -> None:
        while True:
            try:
                await op.execute()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("[%s] falhou, tentando de novo em %.0fs", op.name, op.interval)
            await asyncio.sleep(op.interval)

    async def run(self) -> None:
        for op in self.operations:
            await op.start()
            log.info("operation '%s' iniciada (a cada %.0fs)", op.name, op.interval)

        self._tasks = [asyncio.create_task(self._loop(op)) for op in self.operations]
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for op in self.operations:
                await op.stop()
