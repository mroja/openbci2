#!/usr/bin/env python3

import zmq
import asyncio
from peer import BasicPeer


class Broker(BasicPeer):
    def __init__(self, broker_bind_urls, **kwargs):
        kwargs['peer_id'] = 0
        super().__init__(**kwargs)
        
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


    async def heartbeat_monitor(self):
        while True:
            all_ok = True
            now = time.time()
            for peer_id, last_heartbeat_time in self._heartbeats:
                if now - last_heartbeat_time > 1.0:
                    all_ok = False
                    break


    async def handle_system_request(self, msg):
        return b''
        
        
    async def recv_system_msg(self):
        poller = zmq.asyncio.Poller()
        poller.register(self._broker, zmq.POLLIN)
        while True:
            events = await poller.poll(timeout=100)  # in milliseconds
            
            if len(events) == 0 and not self._running:
                break
            
            if self._broker in dict(events):
                msg = await self._broker.recv_multipart()
                response = await self.handle_system_request(msg)
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

