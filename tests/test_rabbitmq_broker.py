import docker
import os
import pytest
import asyncio
from datetime import datetime
from unittest.mock import MagicMock
from time import sleep

from rat.types import Task, TaskMessage
from rat.exceptions import RejectMessage
from rat.registry import TasksRegistry
from rat.broker import RabbitMQBroker


RABBITMQ_PORT = 35672
QUEUE_NAME = "test_task_queue"
SKIP_CONTAINER_INIT = "SKIP_CONTAINER_INIT" in os.environ


@pytest.fixture(scope="session")
def docker_client() -> docker.DockerClient:
    return docker.from_env()


@pytest.fixture(scope="session", autouse=True)
def rabbitmq_container(docker_client: docker.DockerClient):
    if SKIP_CONTAINER_INIT:
        yield
        return

    container = docker_client.containers.run(
        "rabbitmq", detach=True, auto_remove=True, ports={"5672": str(RABBITMQ_PORT)}
    )
    retries = 0

    while "Server startup complete".encode() not in container.logs():
        if retries >= 10:
            raise RuntimeError("Cant start RabbitMQ container")
        sleep(1)
        retries += 1

    yield

    container.stop()


@pytest.fixture
def registry() -> TasksRegistry:
    return TasksRegistry()


@pytest.fixture
def mock_task(registry: TasksRegistry) -> Task:
    mock = MagicMock()
    task = Task(name="mock_task", callback=mock, routing_key=QUEUE_NAME)
    registry.register(task)

    return task


@pytest.fixture
async def rabbitmq_broker(registry: TasksRegistry) -> RabbitMQBroker:
    broker = RabbitMQBroker(
        registry=registry, url=f"amqp://guest:guest@0.0.0.0:{RABBITMQ_PORT}/"
    )

    await broker.startup()
    queue = await broker.declare_queue(QUEUE_NAME)
    await queue.purge()

    yield broker

    await broker.shutdown()


@pytest.mark.asyncio
async def test_enqueue_task(rabbitmq_broker: RabbitMQBroker, mock_task: Task):
    await rabbitmq_broker.enqueue_task(mock_task.name, some_arg=3, another_arg="foo")

    queue = await rabbitmq_broker.declare_queue(QUEUE_NAME)
    message = await queue.get()
    task_message = TaskMessage.parse_raw(
        message.body, content_type=message.content_type
    )

    assert task_message.name == "mock_task"
    assert isinstance(task_message.timestamp, datetime)
    assert task_message.kwargs == {"some_arg": 3, "another_arg": "foo"}
    assert len(task_message.uid) == 32


@pytest.mark.asyncio
async def test_consume_task(rabbitmq_broker: RabbitMQBroker, mock_task: Task):
    worker = await rabbitmq_broker.start_worker(QUEUE_NAME)
    await rabbitmq_broker.enqueue_task(mock_task.name, some_arg=3, another_arg="foo")

    mock_task.callback.assert_called_once_with(some_arg=3, another_arg="foo")
    assert await worker.queue.get(fail=False) is None


@pytest.mark.asyncio
async def test_reject_task(rabbitmq_broker: RabbitMQBroker, mock_task: Task):
    async def reject_side_effect(*args, **kwargs) -> None:
        raise RejectMessage()

    mock_task.callback.side_effect = reject_side_effect
    worker = await rabbitmq_broker.start_worker(QUEUE_NAME)
    await rabbitmq_broker.enqueue_task(mock_task.name, some_arg=3, another_arg="foo")

    mock_task.callback.assert_called_once_with(some_arg=3, another_arg="foo")
    assert await worker.queue.get(fail=False) is None


@pytest.mark.asyncio
async def test_reject_task_requeue(rabbitmq_broker: RabbitMQBroker, mock_task: Task):
    async def reject_side_effect(*args, **kwargs) -> None:
        raise RejectMessage(requeue=False)

    mock_task.callback.side_effect = reject_side_effect
    worker = await rabbitmq_broker.start_worker(QUEUE_NAME)
    await rabbitmq_broker.enqueue_task(mock_task.name, some_arg=3, another_arg="foo")

    await asyncio.sleep(1)

    mock_task.callback.assert_called_once_with(some_arg=3, another_arg="foo")
    assert await worker.queue.get(fail=False) is None


@pytest.mark.asyncio
async def test_reject_task_unexpected_exception(
    rabbitmq_broker: RabbitMQBroker, mock_task: Task
):
    async def reject_side_effect(*args, **kwargs) -> None:
        raise Exception()

    mock_task.callback.side_effect = reject_side_effect
    worker = await rabbitmq_broker.start_worker(QUEUE_NAME)
    await rabbitmq_broker.enqueue_task(mock_task.name, some_arg=3, another_arg="foo")

    await asyncio.sleep(1)

    assert mock_task.callback.call_count == 2
    assert await worker.queue.get(fail=False) is None


@pytest.mark.asyncio
async def test_task_timeout(rabbitmq_broker: RabbitMQBroker, mock_task: Task):
    async def reject_side_effect(*args, **kwargs) -> None:
        await asyncio.sleep(1)

    mock_task.callback.side_effect = reject_side_effect
    worker = await rabbitmq_broker.start_worker(QUEUE_NAME)
    await rabbitmq_broker.enqueue_task(
        mock_task.name, timeout=0.1, some_arg=3, another_arg="foo"
    )

    await asyncio.sleep(1)

    assert mock_task.callback.call_count == 2
    assert await worker.queue.get(fail=False) is None


@pytest.mark.asyncio
async def test_worker_close(rabbitmq_broker: RabbitMQBroker, mock_task: Task):
    async def reject_side_effect(*args, **kwargs) -> None:
        await asyncio.sleep(1)

    mock_task.callback.side_effect = reject_side_effect
    worker = await rabbitmq_broker.start_worker(QUEUE_NAME, prefetch_size=1)
    await rabbitmq_broker.enqueue_task(mock_task.name)
    await rabbitmq_broker.enqueue_task(mock_task.name)

    await asyncio.sleep(0.1)
    await worker.close()

    assert mock_task.callback.call_count == 1
    assert await worker.queue.get(fail=False) is not None
