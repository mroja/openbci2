
import asyncio
import threading

import zmq
import zmq.asyncio

from .messages import Message
from .peer import Peer, PeerInitUrls


BROKER_REP_INTERNAL_URL = 'inproc://broker_rep'
BROKER_XPUB_INTERNAL_URL = 'inproc://broker_xpub'
BROKER_XSUB_INTERNAL_URL = 'inproc://broker_xsub'

BROKER_INTERNAL_PEER_PUB_URL = 'inproc://broker_peer_pub'
BROKER_INTERNAL_PEER_REP_URL = 'inproc://broker_peer_rep'


class PeerInfo:
    def __init__(self, config=None):
        self.config = config
        self.pub_urls = []
        self.rep_urls = []
        self.pub_listening_urls = []
        self.rep_listening_urls = []


class MsgProxy:
    def __init__(self, xpub_urls, xsub_urls, io_threads=1, hwm=1000):
        self._ctx = None
        self._xpub_urls = xpub_urls
        self._xsub_urls = xsub_urls
        self._io_threads = io_threads
        self._hwm = hwm
        self._thread = threading.Thread(target=self._run)
        self._thread.daemon = True  # TODO: True or False?
        self._thread.start()

    
    def shutdown(self):
        if self._ctx is not None:
            self._ctx.term()
        self._thread.join()

    
    def _run(self):
        self._ctx = zmq.Context(io_threads=self._io_threads)
        self._xpub = self._ctx.socket(zmq.XPUB)
        self._xsub = self._ctx.socket(zmq.XSUB)

        self._xpub.set_hwm(self._hwm)
        self._xsub.set_hwm(self._hwm)

        for url in self._xpub_urls:
            self._xpub.bind(url)
        
        for url in self._xsub_urls:
            self._xsub.bind(url)

        try:
            zmq.proxy(self._xsub, self._xpub)
        except zmq.ContextTerminated:
            self._xsub.close()
            self._xpub.close()


class BrokerPeer(Peer):
    pass


class Broker:
    def __init__(self, rep_urls, xpub_urls, xsub_urls, io_threads=1, hwm=1000):
        self._running = False

        self._rep_urls = rep_urls + [BROKER_REP_INTERNAL_URL]
        self._xpub_urls = xpub_urls + [BROKER_XPUB_INTERNAL_URL]
        self._xsub_urls = xsub_urls + [BROKER_XSUB_INTERNAL_URL]

        # run XPUB & XSUB proxy in different thread
        self._msg_proxy = MsgProxy(self._xpub_urls,
                                   self._xsub_urls,
                                   io_threads=io_threads,
                                   hwm=hwm)

        urls = PeerInitUrls(pub_urls=[BROKER_INTERNAL_PEER_PUB_URL], 
                            rep_urls=[BROKER_INTERNAL_PEER_REP_URL],
                            broker_rep_url=BROKER_REP_INTERNAL_URL)
        
        self._peer = BrokerPeer(peer_id=0,
                                urls=urls,
                                io_threads=io_threads,
                                hwm=hwm)

        self_peer_info = PeerInfo()
        self_peer_info.pub_urls = [BROKER_INTERNAL_PEER_PUB_URL]
        self_peer_info.rep_urls = [BROKER_INTERNAL_PEER_REP_URL]

        self._peers = { 0: self_peer_info }
        self._query_known_types = {}
        self._query_redirect_types = {}

        self._log_messages = True

        self._thread = threading.Thread(target=self._thread_func,
                                        args=(io_threads, hwm))
        self._thread.daemon = True  # TODO: True or False?
        self._thread.start()


    def shutdown(self):
        self._msg_proxy.shutdown()
        self._peer.shutdown()
        self._thread.join()

    def add_peer(self, peer_id, peer_info):
        self._peers[peer_id] = peer_info

    def _thread_func(self, io_threads, hwm):
        try:
            self._ctx = zmq.asyncio.Context(io_threads=io_threads)
            self._loop = zmq.asyncio.ZMQEventLoop()
            asyncio.set_event_loop(self._loop)
    
            self._rep = self._ctx.socket(zmq.REP)
            for url in self._rep_urls:
               self._rep.bind(url)
            
            self._loop.create_task(self._receive_and_handle_requests())
            self._running = True
            self._loop.run_forever()
        finally:
            self._running = False
            self._loop.close()


    # TODO
    async def handle_query(self, query_msg):
        if 0:
            return b'RESPONSE:'
        elif 0:
            return b'REDIRECT_TO_PEER'
        else:
            return b'UNKNOWN'


    async def handle_request(self, msg):
        if self._log_messages:
            print('broker received message: {}'.format(msg.type))
    
        if msg.type == 'BROKER_HELLO':
            if msg.subtype in self._peers:
                return Message('INVALID_REQUEST', '0', 'Peer with such ID is already registered')

            pi = PeerInfo()
            self._peers[msg.subtype] = pi

            return Message('BROKER_HELLO_RESPONSE', '0', {
                'extra_pub_urls': [],
                'extra_rep_urls': []
            })
        elif msg.type == 'BROKER_REGISTER_PEER':
            if msg.subtype not in self._peers:
                return Message('INVALID_REQUEST', '0', 'Say HELLO first!')

            return Message('BROKER_REGISTER_PEER_RESPONSE', '0', {
                'xpub_url': self._xpub_urls[0]
            })
        else:
            return Message('INVALID_REQUEST', '0', 'Unknown request type')


    async def _receive_and_handle_requests(self):
        poller = zmq.asyncio.Poller()
        poller.register(self._rep, zmq.POLLIN)
        while True:
            events = await poller.poll(timeout=50)  # in milliseconds
            if len(events) == 0 and not self._running:
                break
            if self._rep in dict(events):
                msg = await self._rep.recv_multipart()
                print(msg)
                msg = Message.deserialize(msg)
                response = await self.handle_request(msg)
                await self._rep.send_multipart(response.serialize())

