import abc

from rat.broker import BaseBroker


class Consumer(metaclass=abc.ABCMeta):
    def __init__(self, broker: BaseBroker) -> None:
        self.broker = broker

    def ack(self, tag) -> None:
        raise NotImplementedError

    def nack(self, message):  # pragma: no cover
        """Move a message to the dead-letter queue.

        Parameters:
          message(MessageProxy): The message to reject.
        """
        raise NotImplementedError

    def __next__(self):  # pragma: no cover
        """Retrieve the next message off of the queue.  This method
        blocks until a message becomes available.

        Returns:
          MessageProxy: A transparent proxy around a Message that can
          be used to acknowledge or reject it once it's done being
          processed.
        """
