
import zmq
from basic_peer import BasicPeer


class Peer(BasicPeer):
    def __init__(self, broker_url, **kwargs):
        super().__init__(**kwargs)
        self._broker_url = broker_url

        # TODO: communication channel with broker
        self._broker = self._ctx.socket(zmq.REQ)  # ? self._broker = ctx.socket(zmq.PAIR)
        self._broker.connect(self._broker_url)
        
        ### debug
        print("Broker URL: {}\n".format(self._broker_url))

