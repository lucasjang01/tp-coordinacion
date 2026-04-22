import os
import logging
import bisect
import signal
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])
AGGREGATION_CONTROL_EXCHANGE = f"{AGGREGATION_PREFIX}_control"


class AggregationFilter:

    def __init__(self):
        self.data_input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{AGGREGATION_PREFIX}_queue"
        )
        self.eof_input_control = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_CONTROL_EXCHANGE, [AGGREGATION_CONTROL_EXCHANGE]
        )
        self.eof_output_control = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_CONTROL_EXCHANGE, [AGGREGATION_CONTROL_EXCHANGE]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.eof_thread_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.eof_count_by_client = {}
        self.lock = threading.Lock()
        self.data_idle = threading.Event()
        self.data_idle.set()

    def _process_data(self, fruit, amount, client_id):
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated = fruit_top.pop(i) + fruit_item.FruitItem(fruit, amount)
                bisect.insort(fruit_top, updated)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id, output_queue):
        count = self.eof_count_by_client.get(client_id, 0) + 1
        self.eof_count_by_client[client_id] = count
        if count < SUM_AMOUNT:
            return
        del self.eof_count_by_client[client_id]
        fruit_top = self.fruit_top_by_client.pop(client_id, [])
        result = [(fi.fruit, fi.amount) for fi in reversed(fruit_top)]
        output_queue.send(message_protocol.internal.serialize([result, client_id]))

    def process_data_message(self, message, ack, nack):
        self.data_idle.clear()
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            with self.lock:
                self._process_data(*fields)
        else:
            client_id = fields[0]
            with self.lock:
                self._process_eof(client_id, self.output_queue)
            self.eof_output_control.send(
                message_protocol.internal.serialize([client_id, ID])
            )
        ack()
        self.data_idle.set()

    def process_eof_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        sender_id = fields[1]
        if sender_id == ID:
            ack()
            return
        self.data_idle.wait()
        with self.lock:
            self._process_eof(client_id, self.eof_thread_output_queue)
        ack()

    def handle_sigterm(self, _signum, _frame):
        self.data_input_queue.stop_consuming()
        self.eof_input_control.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        eof_thread = threading.Thread(
            target=self.eof_input_control.start_consuming,
            args=(self.process_eof_message,),
            daemon=True,
        )
        eof_thread.start()
        try:
            self.data_input_queue.start_consuming(self.process_data_message)
        finally:
            self.data_input_queue.close()
            self.eof_input_control.close()
            self.eof_output_control.close()
            self.output_queue.close()
            self.eof_thread_output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
