import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_tops_by_client = {}
        self.eof_count_by_client = {}

    def process_messsage(self, message, ack, nack):
        [result, client_id] = message_protocol.internal.deserialize(message)
        tops = self.partial_tops_by_client.setdefault(client_id, [])
        tops.extend(result)
        count = self.eof_count_by_client.get(client_id, 0) + 1
        self.eof_count_by_client[client_id] = count
        if count < AGGREGATION_AMOUNT:
            ack()
            return
        del self.eof_count_by_client[client_id]
        partial_tops = self.partial_tops_by_client.pop(client_id)
        merged = {}
        for (fruit, amount) in partial_tops:
            merged[fruit] = merged.get(fruit, 0) + amount
        final_top = sorted(merged.items(), key=lambda x: x[1], reverse=True)[:TOP_SIZE]
        self.output_queue.send(message_protocol.internal.serialize([final_top, client_id]))
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
