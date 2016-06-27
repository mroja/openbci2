#!/usr/bin/env python3

import zmq
import asyncio
from peer import BasicPeer


class PeerInfo:
    def __init__(self, config=None):
        self.ready = False
        self.config = config
        self.heartbeat = None
        self.urls = []

        self.next_command = None
        


class Broker(BasicPeer):
    def __init__(self, broker_bind_urls, **kwargs):
        kwargs['peer_id'] = 0
        super().__init__(**kwargs)
        
        self._peers = {}
        self._msg_types = []
        
        self._log_messages = False
        
        self._broker_bind_urls = broker_bind_urls
        
        self._broker = self._ctx.socket(zmq.REP)
        for url in self._broker_bind_urls:
           self._broker.bind(url)
        
        if self._log_peers_info:
            print('Broker URLs:')
            for url in self._broker_bind_urls:
                print(url)
            print('')

        #... self.set_filter(b'\x00')

    async def heartbeat_monitor(self):
        while True:
            all_ok = True
            now = time.time()
            # TODO: fixme
            for peer_id, last_heartbeat_time in self._heartbeats:
                if now - last_heartbeat_time > 1.0:
                    all_ok = False
                    break


    async def handle_system_request(self, sender_id, msg, params):
        if sender_id not in self._peers:
            return b'UNKNOWN_PEER_ID'
            
        peer = self._peers[sender_id]
    
        if msg == 'HEARTBEAT':
            peer.heartbeat = time.time()
            if peer.next_command is not None:
                cmd = peer.next_command
                peer.next_command = None
                return cmd
            else:
                return b'OK'
        elif msg == 'HELLO':
            json.decode()
            params.urls = []
            return peer.config
        elif msg == 'REGISTER_MSG_TYPE':
            pass # send msg type id
        elif msg == 'READY':
            peer.ready = True
            return b'OK'
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


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-bind", help="URLs for broker", type=str, required=True, nargs='+')
    parser.add_argument("--bind", help="URLs for broker's built-in peer", type=str, required=True, nargs='+')
    parser.add_argument("--peers", help="URLs of all other peers", type=str, required=True, nargs='+')
    args = parser.parse_args()

    broker_bind_urls = args.broker_bind
    bind_urls = args.bind
    peer_urls = args.peers

    broker = Broker(broker_bind_urls=broker_bind_urls,
                    bind_urls=bind_urls,
                    peer_urls=peer_urls)
    
    broker.run()

