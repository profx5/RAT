import uuid
from typing import Optional, Dict

from rat.types import TaskMessage, Task
from rat.exceptions import TaskNotFound


class TasksRegistry:
    def __init__(self) -> None:
        self._registry: Dict[str, Task] = {}

    def register(self, task: Task) -> None:
        self._registry[task.name] = task

    def get_task(self, name: str) -> Optional[Task]:
        return self._registry.get(name)

    def build_message(self, name: str, **kwargs) -> TaskMessage:
        task = self.get_task(name)
        if not task:
            raise TaskNotFound(name)

        return TaskMessage(
            name=name,
            uid=uuid.uuid4().hex,
            kwargs=kwargs,
        )
