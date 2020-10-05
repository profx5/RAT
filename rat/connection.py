import asyncio
import aioamqp
from typing import Optional, List
from itertools import cycle

from rat.types import AMQPDsn


class AMQPConnection:
    def __init__(self, urls: List[AMQPDsn], reconnect_timeout: int = 5) -> None:
        self.urls_iterator = cycle(urls)
        self.reconnect_timeout = reconnect_timeout

        self._transport: Optional[asyncio.Transport] = None
        self._protocol: Optional[aioamqp.protocol.AmqpProtocol] = None
        self.connected = asyncio.Event()

    async def _connect(self) -> None:
        url = next(self.urls_iterator)
        self._transport, self._protocol = await aioamqp.connect(
            host=url.host,
            port=url.port,
            login=url.user,
            password=url.password,
            on_error=self._on_connection_error,
        )
        self.connected.set()
        self.channel = await self._protocol.channel()
        await self.channel.queue_declare(queue_name="test_queue")

    async def _on_connection_error(self, exception: Exception) -> None:
        raise exception
        self.connected.clear()
        await asyncio.sleep(self.reconnect_timeout)
        await self.reconnect()

    async def reconnect(self) -> None:
        await self._connect()

    async def startup(self) -> None:
        await self._connect()

    async def shutdown(self) -> None:
        if self._protocol:
            await self._protocol.close()

    async def publish(self, payload: str) -> None:
        await self.channel.basic_publish(payload=payload, exchange_name='', routing_key='test_queue')
