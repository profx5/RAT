from typing import Optional, Dict, Tuple

from rat.types import TaskMessage, Task
from rat.exceptions import TaskNotFound


class TasksRegistry:
    def __init__(self) -> None:
        self._registry: Dict[str, Task] = {}

    def register(self, task: Task) -> None:
        self._registry[task.name] = task

    def get_task(self, name: str) -> Optional[Task]:
        return self._registry.get(name)

    def build_message(
        self, name: str, timeout: Optional[None] = None, **kwargs
    ) -> Tuple[TaskMessage, str]:
        task = self.get_task(name)
        if not task:
            raise TaskNotFound(name)

        timeout = timeout or task.timeout

        return (
            TaskMessage(
                name=name,
                kwargs=kwargs,
                timeout=timeout,
            ),
            task.routing_key,
        )
