
import asyncio
import logging
import threading

import zmq
import zmq.asyncio

from ..utils.zmq import bind_to_urls
from .messages import Message
from .peer import Peer, PeerInitUrls
from .zmq_asyncio_task_manager import ZmqAsyncioTaskManager


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

    def __init__(self, xpub_urls, xsub_urls, io_threads=1, hwm=1000, zmq_context=None):
        if zmq_context is None:
            self._destroy_context = True
            self._ctx = None
        else:
            self._destroy_context = False
            self._ctx = zmq_context

        self._xpub_urls = xpub_urls
        self._xsub_urls = xsub_urls
        self._xpub_listening_urls = []
        self._xsub_listening_urls = []
        self._io_threads = io_threads
        self._hwm = hwm

        self._debug = False

        self._logger = logging.getLogger('MsgProxy')

        self._thread = threading.Thread(target=self._run, name='MsgProxy')
        self._thread.daemon = True  # TODO: True or False?
        self._thread.start()

    def shutdown(self):
        if self._ctx is not None:
            if self._destroy_context:
                self._ctx.destroy()
        self._thread.join()

    def _run(self):
        if self._ctx is None:
            self._ctx = zmq.Context(io_threads=self._io_threads)

        self._xpub = self._ctx.socket(zmq.XPUB)
        self._xsub = self._ctx.socket(zmq.XSUB)

        self._xpub.set_hwm(self._hwm)
        self._xsub.set_hwm(self._hwm)

        self._xpub.set(zmq.LINGER, 0)
        self._xsub.set(zmq.LINGER, 0)

        #self._xpub.set(zmq.XPUB_VERBOSE, 1)

        self._xpub_listening_urls = bind_to_urls(self._xpub, self._xpub_urls)
        self._xsub_listening_urls = bind_to_urls(self._xsub, self._xsub_urls)

        self._logger.info("\nMsgProxy: XPUB: {}\nMsgProxy: XSUB: {}\n"
                          .format(', '.join(self._xpub_listening_urls),
                                  ', '.join(self._xsub_listening_urls)))

        try:
            if self._debug:
                poller = zmq.Poller()
                poller.register(self._xpub, zmq.POLLIN)
                poller.register(self._xsub, zmq.POLLIN)
                while True:
                    events = dict(poller.poll(1000))
                    if self._xpub in events:
                        message = self._xpub.recv_multipart()
                        self._logger.debug("[BROKER_PROXY] subscription message: {}".format(message))
                        self._xsub.send_multipart(message)
                    if self._xsub in events:
                        message = self._xsub.recv_multipart()
                        self._logger.debug("[BROKER_PROXY] publishing message: {}".format(message))
                        self._xpub.send_multipart(message)
            else:
                zmq.proxy(self._xsub, self._xpub)
        except zmq.ContextTerminated:
            self._xsub.close()
            self._xsub = None
            self._xpub.close()
            self._xpub = None


class BrokerPeer(Peer):
    pass


class Broker(ZmqAsyncioTaskManager):

    def __init__(self, rep_urls, xpub_urls, xsub_urls, asyncio_loop=None, zmq_context=None,
                 zmq_io_threads=1, hwm=1000, msg_proxy_io_threads=1, msg_proxy_hwm=1000):
        broker_name = 'Broker'
        self._thread_name = broker_name
        self._logger_name = broker_name

        super().__init__(asyncio_loop, zmq_context, zmq_io_threads)

        self._hwm = hwm

        self._rep = None

        self._rep_urls = rep_urls + [BROKER_REP_INTERNAL_URL]
        self._xpub_urls = xpub_urls + [BROKER_XPUB_INTERNAL_URL]
        self._xsub_urls = xsub_urls + [BROKER_XSUB_INTERNAL_URL]

        self._rep_listening_urls = []

        # run XPUB & XSUB proxy in different thread
        self._msg_proxy = MsgProxy(self._xpub_urls,
                                   self._xsub_urls,
                                   io_threads=msg_proxy_io_threads,
                                   hwm=msg_proxy_hwm)

        self._peer = None

        self._peers = {}

        self._query_types = {}
        self._query_redirect_types = {}

        self._log_messages = True

    def shutdown(self):
        super().shutdown()
        if self._running:
            self._msg_proxy.shutdown()
            self._peer.shutdown()
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()
            self._ctx.destroy()

    def add_peer(self, peer_id, peer_info):
        self._peers[peer_id] = peer_info

    def _thread_func(self, io_threads, hwm):
        try:
            self._rep = self._ctx.socket(zmq.REP)
            self._rep.set_hwm(hwm)
            self._rep.set(zmq.LINGER, 0)

            self._rep_listening_urls = bind_to_urls(self._rep, self._rep_urls)

            self._logger.info("Broker: REP: {}".format(', '.join(self._rep_listening_urls)))

            self._start_internal_peer()

    def __():
            self._rep.close(linger=0)
            self._rep = None

    def _start_internal_peer(self):
        urls = PeerInitUrls(pub_urls=[BROKER_INTERNAL_PEER_PUB_URL],
                            rep_urls=[BROKER_INTERNAL_PEER_REP_URL],
                            broker_rep_url=BROKER_REP_INTERNAL_URL)
        self._peer = BrokerPeer(peer_id=0,
                                urls=urls,
                                io_threads=self._io_threads,
                                hwm=self._hwm,
                                zmq_context=self._ctx)

    async def handle_request(self, msg):
        if self._log_messages:
            self._logger.debug('broker received message {} from {}'.format(msg.type, msg.subtype))

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
                'xpub_url': self._xpub_urls[0],
                'xsub_url': self._xsub_urls[0]
            })
        elif msg.type == 'BROKER_QUERY':
            query_type = msg.data['type']
            if query_type in self._query_types:
                response = {
                    'type': 'response',
                    'data': self._query_types[query_type]()
                }
            elif query_type in self._query_redirect_types:
                response = {
                    'type': 'redirect',
                    'data': self._query_redirect_types[query_type]()
                }
            else:
                response = {
                    'type': 'unknown',
                    'data': ''
                }
            return Message('BROKER_QUERY_RESPONSE', '0', response)
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
                msg = Message.deserialize(msg)
                response = await self.handle_request(msg)
                await self._rep.send_multipart(response.serialize())
