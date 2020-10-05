from typing import Any


class BaseRATException(Exception):
    message = "Unexpected exception"
    code = "unexpected_exception"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(self.message)


class TaskNotFound(BaseRATException):
    code = "task_not_found"

    def __init__(self, name: str) -> None:
        self.message = f"Task {name} not found in registry"
        super().__init__()
