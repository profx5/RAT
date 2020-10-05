import asyncio

from rat.types import Message
from rat.registry import Task
from rat.broker import BaseBroker


class Worker:
    MAX_CONCURRENT_TASKS = 5

    def __init__(self, broker: BaseBroker) -> None:
        self.broker = broker

    async def run(self) -> None:
        pass

    async def process_message(self, message: Message) -> None:
        task = self.broker.registry.get_task(message.name)

        if not task:
            raise RuntimeError("Task not found")

        try:
            await self.execute_task(task, message)
            await self.broker.ack()
        except Exception:
            await self.broker.nack()

    async def execute_task(self, task: Task, message: Message) -> None:
        if asyncio.iscoroutine(task.callable):
            await task.callable(**message.kwargs)
        else:
            task.callable(**message.kwargs)
