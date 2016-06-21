#!/usr/bin/env python3

import zmq
import asyncio
from peer import BasicPeer


class Broker(BasicPeer):
    def __init__(self, broker_bind_urls, **kwargs):
        kwargs['peer_id'] = 0
        super().__init__(**kwargs)
        
        self._broker_bind_urls = broker_bind_urls
        
        # TODO: communication channel with broker
        self._broker = self._ctx.socket(zmq.REP)
        for url in self._broker_bind_urls:
           self._broker.bind(url)
        
        ### debug
        print("Broker URLs:")
        for url in self._broker_bind_urls:
            print(url)
        print("")


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

