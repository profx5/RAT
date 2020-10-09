import abc
from typing import Optional
from logging import getLogger
import aio_pika

from rat.types import TaskMessage
from rat.registry import TasksRegistry
from rat.worker import Worker


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
    async def enqueue_message(self, message: TaskMessage, routing_key: str) -> None:
        raise NotImplementedError

    async def enqueue_task(
        self, task_name: str, timeout: Optional[None] = None, **kwargs
    ) -> None:
        message, routing_key = self.registry.build_message(task_name, timeout, **kwargs)

        await self.enqueue_message(message, routing_key)

        self.logger.info("task_enqueue", extra={"name": task_name, "uid": message.uid})

    @abc.abstractmethod
    async def start_worker(self, queue_name: str) -> Worker:
        raise NotImplementedError


class RabbitMQBroker(BaseBroker):
    def __init__(
        self,
        registry: TasksRegistry,
        url: str,
        exchange_name: Optional[str] = None,
    ):
        super().__init__(registry)
        self.connection = aio_pika.RobustConnection(str(url))
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.worker: Optional[Worker] = None
        self.initialized = False

    def _on_message_returned(
        self, _, message: aio_pika.message.ReturnedMessage
    ) -> None:
        self.logger.error(
            "message_returned",
            extra={"exchange": message.exchange, "routing_key": message.routing_key},
        )

    async def startup(self) -> None:
        if self.initialized:
            return

        self.logger.info("broker_startup")
        await self.connection.connect()
        self.channel = await self.connection.channel()
        self.channel.add_on_return_callback(self._on_message_returned)
        self.initialized = True
        self.logger.info("broker_started")

    async def shutdown(self):
        self.logger.info("broker_shutdown")
        if self.worker and not self.worker.closed:
            await self.worker.close()

        for channel in tuple(self.connection._channels.values()):
            await channel.close()
        await self.connection.close()
        self.logger.info("broker_shutedown")

    async def enqueue_message(self, message: TaskMessage, routing_key: str) -> None:
        payload = message.json().encode("utf-8")

        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=payload,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
            mandatory=True,
        )

    async def declare_queue(self, queue_name: str) -> aio_pika.RobustQueue:
        queue = await self.channel.declare_queue(queue_name, durable=True)
        self.logger.info("queue_declared", extra={"queue_name": queue_name})

        return queue

    async def start_worker(
        self,
        queue_name: str,
        prefetch_size: Optional[int] = None,
    ) -> Worker:
        if self.worker is None:
            if prefetch_size:
                await self.channel.set_qos(prefetch_size=prefetch_size)

            queue = await self.declare_queue(queue_name)
            self.worker = await Worker.from_queue(queue, self.registry)

        return self.worker
