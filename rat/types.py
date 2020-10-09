from uuid import uuid4
from typing import Dict, Any, Callable, Union
from datetime import datetime
from pydantic import BaseModel, StrictStr, Field, PositiveInt, PositiveFloat


class Task(BaseModel):
    name: StrictStr
    callback: Callable[[Any], Any]
    routing_key: StrictStr
    timeout: Union[PositiveInt, PositiveFloat, None]


class TaskMessage(BaseModel):
    name: StrictStr
    uid: StrictStr = Field(default_factory=lambda: uuid4().hex)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    kwargs: Dict[str, Any]
    timeout: Union[PositiveInt, PositiveFloat, None]
