import os
import logging
import signal
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{AGGREGATION_PREFIX}_queue"
        )
        self.eof_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )
        self.eof_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )
        self.eof_thread_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{AGGREGATION_PREFIX}_queue"
        )
        self.amount_by_fruit_by_client = {}
        self.lock = threading.Lock()
        self.data_idle = threading.Event()
        self.data_idle.set()

    def _process_data(self, fruit, amount, client_id):
        with self.lock:
            client_fruits = self.amount_by_fruit_by_client.setdefault(client_id, {})
            client_fruits[fruit] = client_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id, output_queue):
        with self.lock:
            client_fruits = self.amount_by_fruit_by_client.pop(client_id, {})
        for final_fruit_item in client_fruits.values():
            output_queue.send(
                message_protocol.internal.serialize(
                    [final_fruit_item.fruit, final_fruit_item.amount, client_id]
                )
            )
        output_queue.send(
            message_protocol.internal.serialize([client_id])
        )

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self.data_idle.clear()
            self._process_data(*fields)
            self.data_idle.set()
        else:
            client_id = fields[0]
            self._process_eof(client_id, self.data_output_queue)
            self.eof_output_exchange.send(message_protocol.internal.serialize([client_id, ID]))
        ack()

    def process_eof_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        sender_id = fields[1]
        if sender_id == ID:
            ack()
            return
        self._process_eof(client_id, self.eof_thread_output_queue)
        ack()

    def handle_sigterm(self, _signum, _frame):
        self.input_queue.stop_consuming()
        self.eof_input_exchange.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        eof_thread = threading.Thread(
            target=self.eof_input_exchange.start_consuming,
            args=(self.process_eof_message,),
            daemon=True,
        )
        eof_thread.start()
        try:
            self.input_queue.start_consuming(self.process_data_message)
        finally:
            self.input_queue.close()
            self.data_output_queue.close()
            self.eof_input_exchange.close()
            self.eof_output_exchange.close()
            self.eof_thread_output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
