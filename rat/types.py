from typing import Dict, Any, Callable
from datetime import datetime
from pydantic import BaseModel, StrictStr, AnyUrl


class Task(BaseModel):
    name: StrictStr
    callback: Callable[[Any], Any]


class TaskMessage(BaseModel):
    name: StrictStr
    uid: StrictStr
    timestamp: datetime = datetime.utcnow
    kwargs: Dict[str, Any]


class AMQPDsn(AnyUrl):
    allowed_schemes = {"amqp"}
