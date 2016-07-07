
import zmq
import asyncio
import threading


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
        self._ctx = zmq.Context(io_threads=io_threads)
        self._xpub = self._ctx.socket(zmq.XPUB)
        self._xsub = self._ctx.socket(zmq.XSUB)

        self._xpub.set_hwm(hwm)
        self._xsub.set_hwm(hwm)

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
    def __init__(self, rep_urls, xpub_urls, xsub_urls, , io_threads=1, hwm=1000):
        self._running = False

        self._rep_urls = rep_urls + [BROKER_REP_INTERNAL_URL]
        self._xpub_urls = xpub_urls + [BROKER_XPUB_INTERNAL_URL]
        self._xsub_urls = xsub_urls + [BROKER_XSUB_INTERNAL_URL]

        # run XPUB & XSUB proxy in different thread
        self._msg_proxy = MsgProxy(self._xpub_urls,
                                   self._xsub_urls,
                                   io_threads=io_threads,
                                   hwm=hwm)

        self._peer = BrokerPeer(peer_id=0,
                                broker_url=BROKER_REP_INTERNAL_URL,
                                io_threads=io_threads,
                                hwm=hwm)

        self_peer_info = PeerInfo()
        self_peer_info.pub_urls = [BROKER_INTERNAL_PEER_PUB_URL]
        self_peer_info.rep_urls = [BROKER_INTERNAL_PEER_REP_URL]

        self._peers = { 0: self_peer_info }
        self._query_known_types = {}
        self._query_redirect_types = {}

        self._log_messages = False

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
            
            self._loop.create_task(self._wait_for_peers())
            self._running = True
            self._loop.run_forever()
        finally:
            self._running = False
            self._loop.close()


    async def heartbeat_monitor(self):
        while True:
            all_ok = True
            now = time.time()
            # TODO: fixme
            for peer_id, last_heartbeat_time in self._heartbeats:
                if now - last_heartbeat_time > 1.0:
                    all_ok = False
                    break

    async def handle_query(self, query_msg):
        if ...:
            return 'RESPONSE:'
        elif ...:
            return 'REDIRECT_TO_PEER'
        else:
            return 'UNKNOWN'

    async def handle_system_request(self, sender_id, msg, params):
        if msg == 'HELLO':
            if peer_id in self._peers:                
                return b'PEER_ID_USED'

            self._peers[peer_id] = 
        
            json.decode()
            params.urls = []
            return peer.config
        elif msg == 'REGISTER_PEER':
            pass # send msg type id
        else:
            return b'INVALID_MESSAGE'


    async def recv_system_msg(self):
        poller = zmq.asyncio.Poller()
        poller.register(self._broker, zmq.POLLIN)
        while True:
            events = await poller.poll(timeout=100)  # in milliseconds
            
            if len(events) == 0 and not self._running:
                break
            
            if self._broker in dict(events):
                msg = await self._broker.recv_multipart()
                if len(msg) > 1:
                    response = await self.handle_system_request(
                        int.from_bytes(msg[0], byteorder='little'), 
                        msg[1],
                        msg[2:] if len(msg) > 2 else None)
                else:
                    response = b'INVALID_MESSAGE'
                await self._broker.send_multipart([response])


