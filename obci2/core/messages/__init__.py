
import json


class Message:
    type_id = None
    types = {}

    def __init__(self, type_id, subtype_id='', data=b''):
        self._type = type_id
        self.subtype = subtype_id
        self.data = data
        if self._type in Message.types:
            self.__class__ = Message.types[self._type].class_type
            self.data = Message.get_data_deserializer(self.type)(data)

    # type is read only
    @property
    def type(self):
        return self._type

    def serialize(self):
        return [
            '{}^{}'.format(self.type, self.subtype).encode('utf-8'), 
            Message.get_data_serializer(self.type)(data)
        ]

    @staticmethod
    def deserialize(msg):
        if len(msg) != 2:
            raise Exception('Invalid message format')
        else:
            try:
                type_id, subtype_id = msg[0].decode('utf-8').split('^', maxsplit=1)
            except Exception:
                raise Exception('Invalid type or subtype')
            data = msg[1]
            return Message(type_id, subtype_id, data)

    @staticmethod
    def serialize_data(data):
        raise Exception('Must be reimplemented in subclass')
    
    @staticmethod
    def deserialize_data(data):
        raise Exception('Must be reimplemented in subclass')

    @staticmethod
    def get_data_serializer(msg_type):
        if msg_type in Message.types[msg_type]:
            return Message.types[msg_type].serialize_data
        def default_serializer(data):
            if isinstance(data, bytes):
                return data
            else:
                return json.dumps(data, ensure_ascii=True, separators=(',', ':')).encode('ascii')
        return default_serializer 

    @staticmethod
    def get_data_deserializer(msg_type):
        if msg_type in Message.types[msg_type]:
            return Message.types[msg_type].deserialize_data
        def default_deserializer(data):
            try:
                return json.loads(data.decode('ascii'))     
            except json.JSONDecodeError:
                return data
        return default_deserializer

    @staticmethod
    def register_type(class_obj):
        Message.types[class_obj.type_id] = class_obj

__all__ = ['Message']

