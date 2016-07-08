
import json

class MessageSerializer:
    @staticmethod
    def serialize(data):
        raise Exception('Must be reimplemented in subclass')

    @staticmethod
    def deserialize(data):
        raise Exception('Must be reimplemented in subclass')

class NullMessageSerializer:
    @staticmethod
    def serialize(data):
        return b''

    @staticmethod
    def deserialize(data):
        return b''


class StringMessageSerializer(MessageSerializer):
    @staticmethod
    def serialize(data):
        return data.encode('utf-8')

    @staticmethod
    def deserialize(data):
        return data.decode('utf-8')


class JsonMessageSerializer(MessageSerializer):
    @staticmethod
    def serialize(data):
        return json.dumps(data, ensure_ascii=True, separators=(',', ':')).encode('ascii')

    @staticmethod
    def deserialize(data):
        return json.loads(data.decode('ascii'))


class Message:
    serializers = {}

    def __init__(self, type_id, subtype_id='', data=None):
        self._type = type_id
        self.subtype = subtype_id
        self.data = data

    # type is read only
    @property
    def type(self):
        return self._type

    def serialize(self):
        return [
            '{}^{}'.format(self.type, self.subtype).encode('utf-8'), 
            Message.serializers[self.type].serialize(self.data)
        ]

    @staticmethod
    def deserialize(msg):
        if len(msg) != 2:
            raise Exception('Invalid message format')
        else:
            try:
                type_id, subtype_id = msg[0].decode('utf-8').split('^', maxsplit=1)
            except Exception:
                raise Exception('Invalid message format: invalid type or subtype')
            data = Message.serializers[type_id].deserialize(msg[1])
            return Message(type_id, subtype_id, data)

    @staticmethod
    def register_serializer(msg_type, serializer_class):
        Message.serializers[msg_type] = serializer_class()


#
# serializers for predefined message types
#

Message.register_serializer('INVALID_REQUEST', StringMessageSerializer)
Message.register_serializer('HEARTBEAT', NullMessageSerializer)

#
# broker messages
#

Message.register_serializer('BROKER_HELLO', JsonMessageSerializer)
Message.register_serializer('BROKER_HELLO_RESPONSE', JsonMessageSerializer)

Message.register_serializer('BROKER_REGISTER_PEER', JsonMessageSerializer)
Message.register_serializer('BROKER_REGISTER_PEER_RESPONSE', JsonMessageSerializer)

Message.register_serializer('BROKER_HEARTBEAT', JsonMessageSerializer)
Message.register_serializer('BROKER_HEARTBEAT_RESPONSE', JsonMessageSerializer)

Message.register_serializer('BROKER_QUERY', JsonMessageSerializer)
Message.register_serializer('BROKER_QUERY_RESPONSE', JsonMessageSerializer)
