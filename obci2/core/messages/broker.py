
import collections

from . import Message
from .json import JsonMessage

###

class BrokerHelloMessage(JsonMessage):
    type_id = 'BROKER_HELLO'
    def __init__(self, peer_id):
        super().__init__(BrokerHelloMessage.type_id, peer_id)

BrokerHelloResponseMessageData = collections.namedtuple('BrokerHelloResponseMessageData', 'pub_urls rep_urls')

class BrokerHelloResponseMessage(JsonMessage):
    type_id = 'BROKER_HELLO_RESPONSE'
    def __init__(self, peer_id, pub_urls, rep_urls):
        data = BrokerHelloResponseMessageData(pub_urls, rep_urls)
        super().__init__(BrokerHelloResponseMessage.type_id, peer_id, data)
    
    @staticmethod
    def deserialize_data(data):
        return BrokerHelloResponseMessageData(*JsonMessage.deserialize_data(data))

Message.register_type(BrokerHelloMessage)
Message.register_type(BrokerHelloResponseMessage)

###

class BrokerRegisterPeerMessage(JsonMessage):
    type_id = 'BROKER_REGISTER_PEER'
    def __init__(self, peed_id):
        super().__init__(BrokerHelloMessage.type_id, '', peer_id)

class BrokerRegisterPeerResponseMessage(JsonMessage):
    type_id = 'BROKER_REGISTER_PEER_RESPONSE'
    def __init__(self, pub_urls, rep_urls):
        super().__init__(BrokerHelloResponseMessage.type_id, '', peer_id)

Message.register_type(BrokerRegisterPeerMessage)
Message.register_type(BrokerRegisterPeerResponseMessage)

###

class BrokerHeartbeatMessage(JsonMessage):
    type_id = 'BROKER_HEARTBEAT'
    def __init__(self, peed_id):
        super().__init__(BrokerHelloMessage.type_id, '', peer_id)


class BrokerHeartbeatResponseMessage(JsonMessage):
    type_id = 'BROKER_HEARTBEAT_RESPONSE'
    def __init__(self, pub_urls, rep_urls):
        super().__init__(BrokerHelloResponseMessage.type_id, '', peer_id)

Message.register_type(BrokerHeartbeatMessage)
Message.register_type(BrokerHeartbeatResponseMessage)

###

class BrokerQueryMessage(JsonMessage):
    type_id = 'BROKER_QUERY'
    def __init__(self, peed_id):
        super().__init__(BrokerHelloMessage.type_id, '', peer_id)

class BrokerQueryResponseMessage(JsonMessage):
    type_id = 'BROKER_QUERY_RESPONSE'
    def __init__(self, pub_urls, rep_urls):
        super().__init__(BrokerHelloResponseMessage.type_id, '', peer_id)

Message.register_type(BrokerQueryMessage)
Message.register_type(BrokerQueryResponseMessage)

