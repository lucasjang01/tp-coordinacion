from common import message_protocol


class MessageHandler:

    def __init__(self, client_id):
        self.client_id = client_id

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([fruit, amount, self.client_id])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        [result, client_id] = message_protocol.internal.deserialize(message)
        if client_id != self.client_id:
            return None
        return result
