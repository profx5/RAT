import asyncio
from aio_pika import connect, IncomingMessage, RobustConnection

loop = asyncio.get_event_loop()

from datetime import datetime
from random import randrange

counter = 0

async def on_message(message: IncomingMessage):
    global counter
    async with message.process():
        counter += 1
        print(" [%s] Received message date: %s" % (counter, datetime.utcnow()))
        await asyncio.sleep(randrange(1, 9) / 10)



async def main():
    # Perform connection
    connection = RobustConnection("amqp://guest:guest@0.0.0.0:35672/")
    await connection.connect()

    # Creating a channel
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    # Declaring queue
    queue = await channel.declare_queue("task_queue", durable=True)

    # Start listening the queue with name 'task_queue'
    await queue.consume(on_message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())

    # we enter a never-ending loop that waits for data and runs
    # callbacks whenever necessary.
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
