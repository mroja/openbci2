
import json

from . import Message

class JsonMessage(Message):
    type_id = 'JSON_MESSAGE'
    def __init__(self, subtype_id, json_data):
        super().__init__(JsonMessage.type_id, subtype_id, json_data)

    @staticmethod
    def serialize_data(data):
        return json.dumps(data, ensure_ascii=True, separators=(',', ':')).encode('ascii')

    @staticmethod
    def deserialize_data(data):
        return json.loads(data.decode('ascii'))

