import logging
import threading

import zmq
import zmq.asyncio

from .messages import Message
from .peer import Peer, PeerInitUrls
from .zmq_asyncio_task_manager import ZmqAsyncioTaskManager
from ..utils.zmq import bind_to_urls


class PeerInfo:

    def __init__(self, config=None):
        self.config = config
        self.pub_urls = []
        self.rep_urls = []
        self.pub_listening_urls = []
        self.rep_listening_urls = []


class MsgProxy:

    def __init__(self, xpub_urls, xsub_urls, io_threads=1, hwm=1000):
        self._xpub_urls = xpub_urls
        self._xsub_urls = xsub_urls
        self._xpub_listening_urls = []
        self._xsub_listening_urls = []
        self._io_threads = io_threads
        self._hwm = hwm
        self._ctx = zmq.Context(io_threads=self._io_threads)

        self._debug = False

        self._logger = logging.getLogger('MsgProxy')

        self._thread = threading.Thread(target=self._run, name='MsgProxy')
        self._thread.daemon = True
        self._thread.start()

    def shutdown(self):
        self._ctx.term()
        self._thread.join()

    def _run(self):

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
                        if self._logger.isEnabledFor(logging.DEBUG):
                            self._logger.debug("[BROKER_PROXY] subscription message: {}".format(message))
                        self._xsub.send_multipart(message)
                    if self._xsub in events:
                        message = self._xsub.recv_multipart()
                        if self._logger.isEnabledFor(logging.DEBUG):
                            self._logger.debug("[BROKER_PROXY] publishing message: {}".format(message))
                        self._xpub.send_multipart(message)
            else:
                zmq.proxy(self._xsub, self._xpub)
        except zmq.ContextTerminated:
            self._xsub.close(linger=0)
            self._xsub = None
            self._xpub.close(linger=0)
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

        self._rep_urls = rep_urls
        self._xpub_urls = xpub_urls
        self._xsub_urls = xsub_urls

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

        self.create_task(self._initialize_broker())

    def add_peer(self, peer_id, peer_info):
        self._peers[peer_id] = peer_info

    async def _initialize_broker(self):
        self._rep = self._ctx.socket(zmq.REP)
        self._rep.set_hwm(self._hwm)
        self._rep.set(zmq.LINGER, 0)

        self._rep_listening_urls = bind_to_urls(self._rep, self._rep_urls)

        self._logger.info("Broker: listening on REP: {}".format(', '.join(self._rep_listening_urls)))

        urls = PeerInitUrls(pub_urls=['tcp://127.0.0.1:*'],
                            rep_urls=['tcp://127.0.0.1:*'],
                            broker_rep_url=self._rep_listening_urls[0])
        self._peer = BrokerPeer(peer_id=0,
                                urls=urls,
                                zmq_io_threads=self._zmq_io_threads,
                                hwm=self._hwm,
                                zmq_context=self._ctx)

        self.create_task(self._receive_and_handle_requests())

    def _cleanup(self):
        self._peer.shutdown()
        self._rep.close(linger=0)
        self._rep = None
        self._msg_proxy.shutdown()
        super()._cleanup()

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
            return await self.handle_query(msg)
        else:
            return Message('INVALID_REQUEST', '0', 'Unknown request type')

    async def handle_query(self, msg):
        query_type = msg.data['type']
        if query_type in self._query_types:
            response = {
                'type': 'response',
                'data': self._query_types[query_type](msg)
            }
        elif query_type in self._query_redirect_types:
            response = {
                'type': 'redirect',
                'data': self._query_redirect_types[query_type](msg)
            }
        else:
            response = {
                'type': 'unknown',
                'data': ''
            }
        return Message('BROKER_QUERY_RESPONSE', '0', response)

    async def _receive_and_handle_requests(self):
        poller = zmq.asyncio.Poller()
        poller.register(self._rep, zmq.POLLIN)
        while True:
            events = await poller.poll(timeout=50)  # in milliseconds
            if self._rep in dict(events):
                msg = await self._rep.recv_multipart()
                msg = Message.deserialize(msg)
                response = await self.handle_request(msg)
                await self._rep.send_multipart(response.serialize())
