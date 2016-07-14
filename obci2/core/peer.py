
import time
import asyncio
import traceback
import threading
from collections import namedtuple

import zmq

from ..utils.asyncio import Timeout
from ..utils.zmq import bind_to_urls, recv_multipart_with_timeout
from .messages import Message
from .message_statistics import MsgPerfStats


PeerInitUrls = namedtuple('PeerInitUrls', ['pub_urls', 'rep_urls', 'broker_rep_url'])


class Peer:

    def __init__(self, peer_id, urls, io_threads=1, hwm=1000, zmq_context=None):
        assert isinstance(urls, (str, PeerInitUrls))

        self._id = peer_id
        self._running = False

        if zmq_context is None:
            self._destroy_context = True
            self._ctx = None
        else:
            self._destroy_context = False
            self._ctx = zmq_context

        self._pub = None
        self._sub = None
        self._req = None
        self._rep = None

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

        ###
        # peer's asyncio message loop runs in a thread
        ###
        self._thread = threading.Thread(target=self._thread_func,
                                        args=(io_threads, hwm))
        self._thread.daemon = True  # TODO: True or False?
        self._thread.start()

    def call_threadsafe(self, func):
        if self._loop is not None:
            self._loop.call_soon_threadsafe(func)

    def shutdown(self):
        if self._running:
            self.call_threadsafe(self._loop.stop)
            self._thread.join()

    def _thread_func(self, io_threads, hwm):
        try:
            if self._ctx is None:
                self._ctx = zmq.asyncio.Context(io_threads=io_threads)
            self._loop = zmq.asyncio.ZMQEventLoop()
            asyncio.set_event_loop(self._loop)

            # PUB socket for sending messages to broker XSUB
            self._pub = self._ctx.socket(zmq.PUB)

            # SUB socket fro receiving messages from broker's XPUB
            self._sub = self._ctx.socket(zmq.SUB)

            # synchronous connection to broker
            self._req = self._ctx.socket(zmq.REQ)

            # synchronous requests from peers
            self._rep = self._ctx.socket(zmq.REP)

            self._pub.set_hwm(hwm)
            self._sub.set_hwm(hwm)
            self._req.set_hwm(hwm)
            self._rep.set_hwm(hwm)

            self._pub.set(zmq.LINGER, 0)
            self._sub.set(zmq.LINGER, 0)
            self._req.set(zmq.LINGER, 0)
            self._rep.set(zmq.LINGER, 0)

            self._loop.create_task(self._connect_to_broker_wrapper())
            self._running = True
            self._loop.run_forever()
        except Exception as ex:
            print(ex)
        finally:
            try:
                self._running = False
                print("Peer '{}' message loop finished".format(self._id))

                tasks = asyncio.gather(*asyncio.Task.all_tasks())
                tasks.cancel()

                try:
                    self._loop.run_until_complete(tasks)
                except Exception:
                    pass

                self._pub.close(linger=0)
                self._pub = None
                self._sub.close(linger=0)
                self._sub = None
                self._req.close(linger=0)
                self._req = None
                self._rep.close(linger=0)
                self._rep = None

                self._loop.close()
                if self._destroy_context:
                    self._ctx.destroy()
                    print("Peer '{}': context destroyed".format(self._id))

                print("Peer '{}': thread finished".format(self._id))
            except Exception as ex:
                print('final error: {}'.format(ex))

    async def _connect_to_broker_wrapper(self):
        try:
            await self._connect_to_broker()
        except Exception as ex:
            print("Peer '{}': init failed: {}: {}".format(self._id, type(ex), ex))
            print(traceback.format_exc())
        else:
            self._loop.create_task(self.heartbeat())
            self._loop.create_task(self.initialization_finished())

    async def _connect_to_broker(self):
        self._pub_listening_urls = bind_to_urls(self._pub, self._pub_urls)
        self._rep_listening_urls = bind_to_urls(self._rep, self._rep_urls)

        #self._pub.connect(self._broker_xpub_url)

        if self._log_peers_info:
            msg = ("\n"
                   "Peer '{}': Initial PUB & REP bind finished.\n"
                   "PUB: {}\n"
                   "REP: {}\n"
                   "\n").format(self._id,
                                ', '.join(self._pub_listening_urls),
                                ', '.join(self._rep_listening_urls))
            print(msg)

        # TODO: implement self._ip_autodiscovery = True
        if self._ip_autodiscovery:
            raise Exception('self._ip_autodiscovery = True not implemented')
        else:
            self._req.connect(self._broker_rep_url)
            print("Peer '{}': Connected to Broker's REP: {}".format(self._id, self._broker_rep_url))

        # send hello to broker, receive extra URLs to bind PUB and REP sockets to
        response = await self.send_broker_message(
            Message('BROKER_HELLO', self._id, {
                'pub_urls': self._pub_listening_urls,
                'rep_urls': self._rep_listening_urls
            })
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
            print(msg)

        # after binding PUB and REP sockets send real URLs to the broker
        # and receive broker's XPUB port to connect SUB to
        response = await self.send_broker_message(
            Message('BROKER_REGISTER_PEER', self._id, {
                'pub_urls': self._pub_listening_urls,
                'rep_urls': self._rep_listening_urls
            }))
        self._broker_xpub_url = response.data['xpub_url']
        self._broker_xsub_url = response.data['xsub_url']
        self._sub.connect(self._broker_xpub_url)
        self._pub.connect(self._broker_xsub_url)
        #self._sub.subscribe(b'')

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
            print(msg)

    async def initialization_finished(self):
        self._loop.create_task(self._receive_sync_messages())
        self._loop.create_task(self._receive_async_messages())

    def set_filter(self, msg_filter):
        self._sub.subscribe(msg_filter.encode('utf-8'))

    def remove_filter(self, msg_filter):
        self._sub.unsubscribe(msg_filter.encode('utf-8'))

    async def heartbeat(self):
        hb_msg = Message('HEARTBEAT', self._id)
        while True:
            heartbeat_timestamp = time.time()

            if self._heartbeat_enabled:
                await self.send_message(hb_msg)

            if not self._running:
                break

            sleep_duration = self._heartbeat_delay - (time.time() - heartbeat_timestamp)
            if sleep_duration < 0:
                sleep_duration = 0
            await asyncio.sleep(sleep_duration)

            # print(self._running)
            # print_threads()
            if not self._running:
                break

    async def send_broker_message(self, msg):
        await self._req.send_multipart(msg.serialize())
        response = await recv_multipart_with_timeout(self._req)
        return Message.deserialize(response)

    async def send_message_to_peer(self, url, msg):
        req = self._ctx.socket(zmq.REQ)
        req.connect(url)
        await req.send_multipart(msg.serialize())
        response = await req.recv_multipart()
        req.close()
        return Message.deserialize(response)

    async def send_message(self, msg):
        if self._log_messages:
            print("peer '{}': sending: type '{}', subtype '{}'"
                  .format(self._id,
                          msg.type,
                          msg.subtype))
        serialized_msg = msg.serialize()
        if self._calc_send_stats:
            self._send_stats.msg(serialized_msg)
        await self._pub.send_multipart(serialized_msg)

    async def query(self, query_type, query_params={}):
        query_msg = Message(
            'BROKER_QUERY', self._id, {
                'type': query_type,
                'params': query_params
        })

        broker_response = await self.send_broker_message(query_msg)

        if broker_response.data['type'] == 'response':
            return broker_response.data['data']
        elif broker_response.data['type'] == 'redirect':
            url = broker_response.data['data']

            redirects = 0
            while True:
                response = await self.send_message_to_peer(url, query_msg)

                if response.data['type'] == 'response':
                    return broker_response.data['data']
                elif response.data['type'] == 'redirect':
                    url = broker_response.data['data']
                else:
                    return None

                redirects += 1
                if redirects >= _max_query_redirects:
                    raise Exception('max redirects reached')
        else:
            return None

    async def handle_sync_message(self, msg):
        if self._log_messages:
            print("peer '{}', received: type '{}', subtype: '{}'"
                  .format(self._id,
                          msg.type,
                          msg.subtype))
        return Message('INVALID_REQUEST', self._id, 'Handler not implemented')

    async def handle_async_message(self, msg):
        if self._log_messages:
            print("peer '{}', received: type '{}', subtype: '{}'"
                  .format(self._id,
                          msg.type,
                          msg.subtype))

    async def _receive_sync_messages_handler(self):
        msg = await self._rep.recv_multipart()
        replay = await self.handle_sync_message(Message.deserialize(msg))
        await self._rep.send_multipart(replay.serialize())

    async def _receive_async_messages_handler(self):
        msg = await self._sub.recv_multipart()
        msg = Message.deserialize(msg)
        if self._calc_recv_stats:
            self._recv_stats.msg(msg)
        await self.handle_async_message(msg)

    async def _receive_messages_helper(self, socket, handler):
        poller = zmq.asyncio.Poller()
        poller.register(socket, zmq.POLLIN)
        while True:
            events = await poller.poll(timeout=50)  # in milliseconds
            if len(events) == 0 and not self._running:
                break
            if socket in dict(events):
                await handler()

    async def _receive_sync_messages(self):
        await self._receive_messages_helper(self._rep, self._receive_sync_messages_handler)

    async def _receive_async_messages(self):
        await self._receive_messages_helper(self._sub, self._receive_async_messages_handler)
