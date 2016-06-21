
import asyncio
import zmq
import zmq.asyncio


class BasicPeer:
    def __init__(self, peer_id, bind_urls, peer_urls, io_threads=1):
        self._id = peer_id
        self._peer_urls = peer_urls
        self._bind_urls = bind_urls
        
        # listening URLs after binding (e.g. with specific port
        # numbers when * was given as port number or as IP address)
        self._listening_urls = []

        self._ctx = zmq.asyncio.Context(io_threads=io_threads)
        self._loop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(self._loop)

        # PUB socket for sending messages to other peers
        self._pub = self._ctx.socket(zmq.PUB)

        for url in self._bind_urls:
            self._pub.bind(url)
            real_url = self._pub.getsockopt(zmq.LAST_ENDPOINT)
            if real_url:
                self._listening_urls.append(real_url.decode())

        # SUB socket is subscribed to every other peer
        self._sub = self._ctx.socket(zmq.SUB)

        for url in self._peer_urls:
            self._sub.connect(url)

        # TODO: receive no messages by default
        # receive all messages by default
        self._sub.setsockopt(zmq.SUBSCRIBE, b'')

        ### debug
        print("")
        print("Peer {}".format(self._id))
        print("\nListening URLs:")
        for url in self._listening_urls:
            print(url)
        print("\nPeer URLs:")
        for url in self._peer_urls:
            print(url)
        print("")


    async def send_message(self, msg_type, data):
        print('peer: {}, sending: type: {}, data: {}'.format(self._id, msg_type, data))
        await self._pub.send_multipart([msg_type.to_bytes(2, byteorder='little'), 
                                        self._id.to_bytes(2, byteorder='little'), 
                                        data])
        
        
    async def handle_message(self, msg_type, sending_peer, msg_data):
        print('peer: {}, received: type: {}, from: {}, data: {}'.
                format(self._id, msg_type, sending_peer, msg_data))


    async def recv_and_process(self):
        poller = zmq.asyncio.Poller()
        poller.register(self._sub, zmq.POLLIN)
        while True:
            events = await poller.poll(timeout=100)  # in milliseconds
            
            if len(events) == 0 and 0: # TODO: exit condition
                break
            
            if self._sub in dict(events):
                msg_type, sending_peer, msg_data = await self._sub.recv_multipart()
                
                msg_type = int.from_bytes(msg_type, byteorder='little')
                sending_peer = int.from_bytes(sending_peer, byteorder='little')
                
                await self.handle_message(msg_type, sending_peer, msg_data)


    def run(self):
        self._loop.create_task(self.recv_and_process())
        
        try:
            self._loop.run_forever()
        finally:
            self._loop.close()

