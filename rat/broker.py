import abc
import asyncio
from logging import getLogger
from typing import List

from rat.types import Message, AMQPDsn
from rat.registry import TasksRegistry
from rat.connection import AMQPConnection


class BaseBroker(metaclass=abc.ABCMeta):
    def __init__(self, registry: TasksRegistry):
        self.logger = getLogger(f"broker.{self.__class__.__name__}")
        self.registry = registry

    @abc.abstractmethod
    async def startup(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def shutdown(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def enqueue_message(self, message: Message) -> None:
        raise NotImplementedError

    async def enqueue_task(self, task_name: str, **kwargs) -> None:
        message = self.registry.build_message(task_name, **kwargs)

        await self.enqueue_message(message)

    @abc.abstractmethod
    async def flush(self) -> None:
        raise NotImplementedError


class AsyncQueueBroker(BaseBroker):
    QUEUE_MAX_SIZE = 100

    def __init__(self, *args, **kwargs) -> None:
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=self.QUEUE_MAX_SIZE)
        super().__init__(*args, **kwargs)

    async def startup(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    async def enqueue_message(self, message: Message) -> None:
        await self.queue.put(message.json())

    async def flush(self) -> None:
        while self.queue.qsize() > 0:
            self.queue.get_nowait()

    async def join(self) -> None:
        await self.queue.join()


class RabbitMQBroker(BaseBroker):
    def __init__(self, registry: TasksRegistry, urls: List[AMQPDsn]):
        super().__init__(registry)
        self.connection = AMQPConnection(urls)

    async def startup(self) -> None:
        await self.connection.startup()

    async def shutdown(self):
        await self.connection.shutdown()

    async def enqueue_message(self, message: Message) -> None:
        await self.connection.connected.wait()
        await self.connection.publish(message.json())

    async def flush(self) -> None:
        pass

    async def start_consuming(self) -> None:
        async def callback(channel, body, envelope, properties):
            import ipdb; ipdb.set_trace()
            print("ASDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDd")
            msg = Message.parse_raw(body, content_type="application/json")
            await asyncio.sleep(msg.kwargs["some_arg"])

        self.connection.channel.basic_consume(
            callback, queue_name="test_queue"
        )
