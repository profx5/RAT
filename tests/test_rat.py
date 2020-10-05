import pytest
from datetime import datetime
from time import sleep

from rat.types import Message, AMQPDsn
from rat.registry import TasksRegistry, Task
from rat.broker import AsyncQueueBroker, RabbitMQBroker
from rat.worker import Worker


async def coroutine_for_testing(some_arg: int, another_arg: str) -> None:
    print(some_arg)
    print(another_arg)


@pytest.fixture
def stub_registry() -> TasksRegistry:
    registry = TasksRegistry()
    registry.register(Task("test_task", coroutine_for_testing))

    return registry


@pytest.fixture
def stub_broker(stub_registry: TasksRegistry) -> AsyncQueueBroker:
    return AsyncQueueBroker(registry=stub_registry)


from pydantic import BaseModel


class TestModel(BaseModel):
    url: AMQPDsn


@pytest.fixture
def rabbitmq_broker(stub_registry: TasksRegistry) -> RabbitMQBroker:
    return RabbitMQBroker(
        registry=stub_registry,
        urls=[TestModel(url="amqp://guest:guest@127.0.0.1/").url],
    )


# @pytest.fixture
# def stub_worker(stub_registry: TasksRegistry, stub_broker) -> Worker:
#     return Worker(broker)


@pytest.mark.asyncio
@pytest.mark.freeze_time()
async def test_stub_broker_enqueue(stub_broker: AsyncQueueBroker):
    await stub_broker.enqueue_task("test_task", some_arg=123, another_arg="foo")

    assert stub_broker.queue.qsize() == 1
    raw_message = stub_broker.queue.get_nowait()
    message = Message.parse_raw(raw_message, content_type="application/json")
    assert message.name == "test_task"
    assert message.message_timestamp == datetime.utcnow()
    assert message.kwargs == {"some_arg": 123, "another_arg": "foo"}
    assert message.message_uid


@pytest.mark.asyncio
async def test_rabbitmq_broker_enqueue(rabbitmq_broker: RabbitMQBroker):
    await rabbitmq_broker.startup()
    await rabbitmq_broker.enqueue_task("test_task", some_arg=3, another_arg="foo")
    await rabbitmq_broker.enqueue_task("test_task", some_arg=3, another_arg="foo")
    await rabbitmq_broker.enqueue_task("test_task", some_arg=3, another_arg="foo")
    await rabbitmq_broker.shutdown()

@pytest.mark.asyncio
async def test_rabbitmq_broker_consume(rabbitmq_broker: RabbitMQBroker):
    await rabbitmq_broker.startup()
    await rabbitmq_broker.start_consuming()
    import asyncio
    await asyncio.sleep(5)

    # assert stub_broker.queue.qsize() == 1
    # raw_message = stub_broker.queue.get_nowait()
    # message = Message.parse_raw(raw_message, content_type="application/json")
    # assert message.name == "test_task"
    # assert message.message_timestamp == datetime.utcnow()
    # assert message.kwargs == {"some_arg": 123, "another_arg": "foo"}
    # assert message.message_uid


# async def test_worker_with_stub_broker(stub_registry: ,stub_broker: AsyncQueueBroker):
#     await stub_broker.enqueue_task("test_task", some_arg=123, another_arg="foo")

#     worker = Worker(stub)
