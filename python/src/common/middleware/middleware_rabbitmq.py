import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareMessageError,
)


class _RabbitMQBase:
    def _connect(self, host):
        params = pika.ConnectionParameters(host=host, port=5672, heartbeat=60)
        try:
            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()
        except Exception as e:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ") from e
        self._consuming = False

    def _wrap_callback(self, on_message_callback):
        def _pika_callback(ch, method, properties, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            try:
                on_message_callback(body, ack, nack)
            except Exception as e:
                raise MessageMiddlewareMessageError("Error in message callback") from e
        return _pika_callback

    def start_consuming(self, on_message_callback):
        self._consuming = True
        self._channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=self._wrap_callback(on_message_callback),
            auto_ack=False,
        )
        try:
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Connection lost while consuming") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error during message consumption") from e
        finally:
            self._consuming = False

    def stop_consuming(self):
        if not self._consuming:
            return
        self._channel.stop_consuming()

    def send(self, message):
        raise NotImplementedError("Subclasses must implement send()")

    def close(self):
        errors = []
        try:
            self._channel.close()
        except Exception as e:
            errors.append(e)
        try:
            self._connection.close()
        except Exception as e:
            errors.append(e)
        if errors:
            raise MessageMiddlewareCloseError(errors)


class MessageMiddlewareQueueRabbitMQ(_RabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue_name = queue_name
        self._connect(host)
        try:
            self._channel.confirm_delivery()
            self._channel.queue_declare(queue=queue_name, durable=True)
            self._channel.basic_qos(prefetch_count=1)
        except pika.exceptions.AMQPError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError("Failed to declare queue") from e

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue_name,
                body=message,
            )
        except Exception as e:
            raise MessageMiddlewareMessageError("Failed to send message to queue") from e


class MessageMiddlewareExchangeRabbitMQ(_RabbitMQBase, MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._connect(host)
        try:
            self._channel.confirm_delivery()
            self._setup_exchange()
            self._setup_queue_and_bindings()
        except Exception:
            self.close()
            raise

    def _setup_exchange(self):
        try:
            self._channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type='direct',
            )
        except Exception as e:
            raise MessageMiddlewareDisconnectedError("Failed to declare exchange") from e

    def _setup_queue_and_bindings(self):
        try:
            result = self._channel.queue_declare(queue='', exclusive=True)
            self._queue_name = result.method.queue
            for routing_key in self._routing_keys:
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=self._queue_name,
                    routing_key=routing_key,
                )
        except Exception as e:
            raise MessageMiddlewareDisconnectedError("Failed to set up queue bindings") from e

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=self._routing_keys[0],
                body=message,
            )
        except Exception as e:
            raise MessageMiddlewareMessageError("Failed to send message to exchange") from e
