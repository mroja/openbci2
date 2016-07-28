import asyncio
import time
from collections import namedtuple

import zmq

from .message_statistics import MsgPerfStats
from .messages import Message
from .zmq_asyncio_task_manager import ZmqAsyncioTaskManager
from ..utils.zmq import bind_to_urls, recv_multipart_with_timeout

PeerInitUrls = namedtuple('PeerInitUrls', ['pub_urls', 'rep_urls', 'broker_rep_url'])


class TooManyRedirectsException(Exception):
    pass


class Peer(ZmqAsyncioTaskManager):

    def __init__(self, peer_id, urls, asyncio_loop=None, zmq_context=None, zmq_io_threads=1, hwm=1000):
        peer_name = 'Peer_{}'.format(peer_id)
        self._thread_name = peer_name
        self._logger_name = peer_name

        super().__init__(asyncio_loop, zmq_context, zmq_io_threads)

        assert isinstance(urls, (str, PeerInitUrls))

        self._id = peer_id

        self._hwm = hwm

        self._pub = None  # PUB socket for sending messages to broker XSUB
        self._sub = None  # SUB socket for receiving messages from broker's XPUB
        self._req = None  # synchronous connection to broker
        self._rep = None  # synchronous requests from peers

        self._broker_rep_url = None
        self._broker_xpub_url = None
        self._broker_xsub_url = None

        # listening URLs after binding (e.g. with specific port
        # numbers when * was given as port number or as IP address)
        self._pub_urls = []
        self._rep_urls = []
        self._pub_listening_urls = []
        self._rep_listening_urls = []

        if isinstance(urls, str):
            self._ip_autodiscovery = True
            self._pub_urls = ['tcp://*:*']
            self._rep_urls = ['tcp://*:*']
            self._broker_tcp_ip_address = urls
            self._broker_rep_url = None  # TODO: fixme
        else:
            self._ip_autodiscovery = False
            assert isinstance(urls.pub_urls, (list, tuple))
            assert isinstance(urls.rep_urls, (list, tuple))
            self._pub_urls = urls.pub_urls
            self._rep_urls = urls.rep_urls
            self._broker_rep_url = urls.broker_rep_url

        ###
        # heartbeat
        ###
        self._heartbeat_enabled = False
        self._heartbeat_delay = 0.05  # 50 ms

        self._max_query_redirects = 10

        ###
        # logs verbosity
        ###
        self._log_messages = True
        self._log_peers_info = True

        ###
        # message statistics
        ###
        stats_interval = 4.0

        # async send statistics
        self._calc_send_stats = False
        self._send_stats = MsgPerfStats(stats_interval, 'SEND')

        # async receive statistics
        self._calc_recv_stats = False
        self._recv_stats = MsgPerfStats(stats_interval, 'RECV')

        self.create_task(self._connect_to_broker())

    def set_filter(self, msg_filter):
        if self._sub is not None:
            self._sub.subscribe(msg_filter.encode('utf-8'))

    def remove_filter(self, msg_filter):
        if self._sub is not None:
            self._sub.unsubscribe(msg_filter.encode('utf-8'))

    def _cleanup(self):
        self._pub.close(linger=0)
        self._sub.close(linger=0)
        self._req.close(linger=0)
        self._rep.close(linger=0)
        self._pub = None
        self._sub = None
        self._req = None
        self._rep = None
        super()._cleanup()

    async def _connect_to_broker(self):
        try:
            self._pub = self._ctx.socket(zmq.PUB)
            self._sub = self._ctx.socket(zmq.SUB)
            self._req = self._ctx.socket(zmq.REQ)
            self._rep = self._ctx.socket(zmq.REP)
            for socket in [self._pub, self._sub, self._req, self._rep]:
                socket.set_hwm(self._hwm)
                socket.set(zmq.LINGER, 0)
            await self.__connect_to_broker_impl()
        except Exception:
            self._logger.exception("initialization failed for peer '{}': ".format(self._id))
            raise
        else:
            self._loop.create_task(self.heartbeat())
            self._loop.create_task(self.initialization_finished())

    async def __connect_to_broker_impl(self):
        self._pub_listening_urls = bind_to_urls(self._pub, self._pub_urls)
        self._rep_listening_urls = bind_to_urls(self._rep, self._rep_urls)

        # self._pub.connect(self._broker_xpub_url)

        if self._log_peers_info:
            msg = ("\n"
                   "Peer '{}': Initial PUB & REP bind finished.\n"
                   "PUB: {}\n"
                   "REP: {}\n"
                   "\n").format(self._id,
                                ', '.join(self._pub_listening_urls),
                                ', '.join(self._rep_listening_urls))
            self._logger.debug(msg)

        # TODO: implement self._ip_autodiscovery = True
        if self._ip_autodiscovery:
            raise Exception('self._ip_autodiscovery = True not implemented')
        else:
            self._req.connect(self._broker_rep_url)
            self._logger.debug("Peer '{}': Connected to Broker's REP: {}".format(self._id, self._broker_rep_url))

        # send hello to broker, receive extra URLs to bind PUB and REP sockets to
        response = await self.send_broker_message(
            Message('BROKER_HELLO', self._id, {
                'pub_urls': self._pub_listening_urls,
                'rep_urls': self._rep_listening_urls
            }),
            timeout=10.0
        )

        self._pub_urls += response.data['extra_pub_urls']
        self._rep_urls += response.data['extra_rep_urls']

        self._pub_listening_urls += bind_to_urls(self._pub, response.data['extra_pub_urls'])
        self._rep_listening_urls += bind_to_urls(self._rep, response.data['extra_rep_urls'])

        if self._log_peers_info:
            msg = ("\n"
                   "Peer '{}': After BROKER_HELLO.\n"
                   "PUB: {}\n"
                   "REP: {}\n"
                   "\n").format(self._id,
                                ', '.join(self._pub_listening_urls),
                                ', '.join(self._rep_listening_urls))
            self._logger.debug(msg)

        # after binding PUB and REP sockets send real URLs to the broker
        # and receive broker's XPUB port to connect SUB to
        response = await self.send_broker_message(
            Message('BROKER_REGISTER_PEER', self._id, {
                'pub_urls': self._pub_listening_urls,
                'rep_urls': self._rep_listening_urls
            }),
            timeout=10.0
        )
        self._broker_xpub_url = response.data['xpub_url']
        self._broker_xsub_url = response.data['xsub_url']
        self._sub.connect(self._broker_xpub_url)
        self._pub.connect(self._broker_xsub_url)

        if self._log_peers_info:
            msg = ("\n"
                   "Peer '{}'. Connect to Broker finished.\n"
                   "Connected to broker at REP {}; XPUB {}\n"
                   "PUB URLs: {}\n"
                   "REP URLs: {}\n"
                   "\n").format(self._id,
                                self._broker_rep_url,
                                self._broker_xpub_url,
                                ', '.join(self._pub_listening_urls),
                                ', '.join(self._rep_listening_urls))
            self._logger.info(msg)

    async def initialization_finished(self):
        self.create_task(self._receive_sync_messages())
        self.create_task(self._receive_async_messages())

    async def heartbeat(self):
        heartbeat_message = Message('HEARTBEAT', self._id)
        while True:
            heartbeat_timestamp = time.monotonic()
            if self._heartbeat_enabled:
                await self.send_message(heartbeat_message)
            sleep_duration = self._heartbeat_delay - (time.monotonic() - heartbeat_timestamp)
            if sleep_duration < 0:
                sleep_duration = 0
            await asyncio.sleep(sleep_duration)

    async def send_broker_message(self, msg, timeout=1.0):
        if self._log_messages:
            self._logger.debug("sending sync message to broker: type '{}', subtype '{}'"
                               .format(msg.type, msg.subtype))
        await self._req.send_multipart(msg.serialize())
        response = await recv_multipart_with_timeout(self._req, timeout=timeout)
        return Message.deserialize(response)

    async def send_message_to_peer(self, url, msg):
        if self._log_messages:
            self._logger.debug("sending sync message to '{}': type '{}', subtype '{}'"
                               .format(url, msg.type, msg.subtype))
        req = self._ctx.socket(zmq.REQ)
        req.connect(url)
        try:
            await req.send_multipart(msg.serialize())
            response = await recv_multipart_with_timeout(req)
        finally:
            req.close(linger=0)
        return Message.deserialize(response)

    async def send_message(self, msg):
        if self._log_messages:
            self._logger.debug("sending async message: type '{}', subtype '{}'"
                               .format(msg.type, msg.subtype))
        serialized_msg = msg.serialize()
        if self._calc_send_stats:
            self._send_stats.msg(serialized_msg)
        await self._pub.send_multipart(serialized_msg)

    async def query(self, query_type, query_params={}):
        query_msg = Message('BROKER_QUERY', self._id, { 'type': query_type, 'params': query_params })
        broker_response = await self.send_broker_message(query_msg)
        if broker_response.data['type'] == 'response':
            return broker_response.data['data']
        elif broker_response.data['type'] == 'redirect':
            url = broker_response.data['data']
            redirects = 0
            while True:
                response = await self.send_message_to_peer(url, query_msg)
                if response.data['type'] == 'response':
                    return response.data['data']
                elif response.data['type'] == 'redirect':
                    url = response.data['data']
                else:
                    return None
                redirects += 1
                if redirects >= self._max_query_redirects:
                    self._logger.error("max redirects ({}) reached when executing query '{}'"
                                       .format(self._max_query_redirects, query_type))
                    raise TooManyRedirectsException('max redirects reached')
        else:
            return None

    async def handle_sync_message(self, msg):
        return Message('INTERNAL_ERROR', self._id, 'Handler not implemented')

    async def handle_async_message(self, msg):
        pass

    async def _receive_sync_messages(self):
        async def sync_handler():
            msg = Message.deserialize(await self._rep.recv_multipart())
            if self._log_messages:
                self._logger.debug("received sync message: type '{}', subtype: '{}'"
                                   .format(msg.type, msg.subtype))
            response = await self.handle_sync_message(msg)
            await self._rep.send_multipart(response.serialize())
        await self.__receive_messages_helper(self._rep, sync_handler)

    async def _receive_async_messages(self):
        async def async_handler():
            msg = Message.deserialize(await self._sub.recv_multipart())
            if self._calc_recv_stats:
                self._recv_stats.msg(msg)
            if self._log_messages:
                self._logger.debug("received async message: type '{}', subtype: '{}'"
                                   .format(msg.type, msg.subtype))
            await self.handle_async_message(msg)
        await self.__receive_messages_helper(self._sub, async_handler)

    @staticmethod
    async def __receive_messages_helper(socket, handler):
        """
        Two concurrent polling loops are run (for SUB and REP) to avoid one message type processing blocking another.
        """
        poller = zmq.asyncio.Poller()
        poller.register(socket, zmq.POLLIN)
        while True:
            events = await poller.poll(timeout=100)  # timeout in milliseconds
            if socket in dict(events):
                await handler()
