
import time
import asyncio

import zmq
import zmq.asyncio


#HWM = 1000
HWM = 2


class MsgPerfStats:
    def __init__(self, interval, name=''):
        self._name = name
        self.interval = interval

    def reset(self):
        self._last_time = time.time()
        self._start_time = self._last_time
        self._count = 0
        self._total_size = 0

    def msg(self, msg):
        self._last_time = time.time()
        self._count += 1
        self._total_size += sum(map(len, msg))
        measurement_time = self._last_time - self._start_time
        
        if measurement_time > self._interval:
            mean_size = int(self._total_size / self._count)
            messages_per_second = self._count / measurement_time
            megabytes_per_second = (self._total_size / measurement_time) / 1e6
            
            if self._name:
                print('stats for "{}"'.format(self._name))
            print('message count:     {:6d} [msgs]'.format(self._count))
            print('mean message size: {:6d} [B]'.format(mean_size))
            print('mean throughput:   {:4.2f} [msg/s]'.format(messages_per_second))
            print('mean throughput:   {:2.4f} [MB/s]'.format(megabytes_per_second))                
            print('measurement time:  {:2.4f} [s]'.format(measurement_time))
            print('')
            
            self.reset()      

    @property
    def interval(self):
        return self._interval
    
    @interval.setter
    def interval(self, interval):
        self._interval = interval
        self.reset()


class BasicPeer:
    def __init__(self, peer_id, bind_urls, peer_urls, io_threads=1):
        self._running = True
        
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
        
        # SUB socket is subscribed to every other peer
        self._sub = self._ctx.socket(zmq.SUB)

        self._pub.setsockopt(zmq.SNDHWM, HWM)
        #self._pub.setsockopt(zmq.RCVHWM, HWM)
        self._sub.setsockopt(zmq.SNDHWM, HWM)
        self._sub.setsockopt(zmq.RCVHWM, HWM)

        for url in self._bind_urls:
            self._pub.bind(url)
            real_url = self._pub.getsockopt(zmq.LAST_ENDPOINT)
            if real_url:
                self._listening_urls.append(real_url.decode())

        for url in self._peer_urls:
            self._sub.connect(url)        
        
        self._log_messages = False
        self._log_peers_info = True

        self._calc_send_stats = True
        self._send_stats = MsgPerfStats(4.0, 'SEND')
        
        self._calc_recv_stats = True
        self._recv_stats = MsgPerfStats(4.0, 'RECV')

        if self._log_peers_info:
            print('')
            print('Peer {}'.format(self._id))
            print("\nListening URLs:")
            for url in self._listening_urls:
                print(url)
            print("\nPeer URLs:")
            for url in self._peer_urls:
                print(url)
            print('')

    def set_filter(self, msg_filter):
        self._sub.setsockopt(zmq.SUBSCRIBE, msg_filter)

    def remove_filter(self, msg_filter):
        self._sub.setsockopt(zmq.UNSUBSCRIBE, msg_filter)

    async def send_message(self, msg_type, data):
        if self._log_messages:
            print('peer: {}, sending: type: {}, data: {}'.format(self._id, msg_type, data))
        
        msg = [msg_type.to_bytes(2, byteorder='little'), 
               self._id.to_bytes(2, byteorder='little'), 
               data,
               data * 200] # FOR TEST ONLY
        
        await self._pub.send_multipart(msg)
        
        if self._calc_send_stats:
            self._send_stats.msg(msg)

        
    async def handle_message(self, msg_type, sending_peer, msg_data):
        if self._log_messages:
            print('peer: {}, received: type: {}, from: {}, data: {}'.
                    format(self._id, msg_type, sending_peer, msg_data))


    async def recv_and_process(self):
        poller = zmq.asyncio.Poller()
        poller.register(self._sub, zmq.POLLIN)
        while True:
            events = await poller.poll(timeout=100)  # in milliseconds
            
            if len(events) == 0 and not self._running:
                break
            
            if self._sub in dict(events):
                msg = await self._sub.recv_multipart()
                
                if self._calc_recv_stats:
                    self._recv_stats.msg(msg)
                
                msg_type = int.from_bytes(msg[0], byteorder='little')
                sending_peer = int.from_bytes(msg[1], byteorder='little')
                
                await self.handle_message(msg_type, sending_peer, msg[2])


    def run(self):
        self._loop.create_task(self.recv_and_process())
        
        try:
            self._loop.run_forever()
        finally:
            self._loop.close()

