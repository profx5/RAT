import asyncio
import aio_pika
from typing import Optional
from logging import getLogger

from rat.registry import TasksRegistry
from rat.types import TaskMessage
from rat.exceptions import NackMessage, RejectMessage


class Worker:
    def __init__(
        self,
        registry: TasksRegistry,
        queue: aio_pika.Queue,
    ) -> None:
        self.registry = registry
        self.queue = queue
        self.loop = queue._channel.loop
        self.consumer_tag: Optional[str] = None

        self.closing = False
        self.closed = False
        self.processing_lock = asyncio.Lock()

        self.logger = getLogger(f"rat.worker.{queue.name}")
        self.logger.info("worker_started", {"queue_name": queue.name})

    @classmethod
    async def from_queue(
        cls,
        queue: aio_pika.Queue,
        registry: TasksRegistry,
    ) -> "Worker":
        worker = cls(registry, queue)
        worker.consumer_tag = await queue.consume(worker.process_message)

        return worker

    async def process_message(self, message: aio_pika.IncomingMessage) -> None:
        if self.closing:
            await message.nack(requeue=True)

            return

        task_message = TaskMessage.parse_raw(
            message.body, content_type=message.content_type
        )

        task = self.registry.get_task(task_message.name)
        if not task:
            self.logger.error(
                "task_not_found",
                extra={
                    "task_name": task_message.name,
                },
            )
            message.reject(requeue=False)

            return

        async with message.process(reject_on_redelivered=True, requeue=True):
            logging_extra = {"name": task_message.name, "uid": task_message.uid}
            self.logger.info(
                "task_processing_start",
                extra=logging_extra,
            )
            try:
                await self.processing_lock.acquire()
                await asyncio.wait_for(
                    task.callback(**task_message.kwargs), timeout=task_message.timeout
                )
            except RejectMessage as e:
                await message.reject(requeue=e.requeue)
                self.logger.error(
                    "task_rejected", extra=dict(logging_extra, requeue=e.requeue)
                )
            except NackMessage as e:
                await message.nack(requeue=e.requeue)
                self.logger.error(
                    "task_nack", extra=dict(logging_extra, requeue=e.requeue)
                )
            except asyncio.TimeoutError:
                self.logger.error(
                    "task_timeout",
                    extra=dict(logging_extra, timeout=task_message.timeout),
                )
                raise
            except Exception:
                self.logger.exception(
                    "error_while_processing_task", extra=logging_extra
                )
                raise
            finally:
                self.processing_lock.release()

    def close(self) -> asyncio.Task:
        self.logger.info("worker_close")
        self.canceling = True

        async def closer():
            if not self.consumer_tag:
                return

            await self.queue.cancel(self.consumer_tag, timeout=1)
            await self.processing_lock.acquire()
            self.closed = True
            self.logger.info("worker_closed")

        return self.loop.create_task(closer())
