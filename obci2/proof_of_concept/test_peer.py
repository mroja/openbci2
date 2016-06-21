
import os
import sys

from peer import Peer


class TestPeer(Peer):
    def __init__(self, **kwargs):
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--id", help="unique peer id", type=int, required=True)
        parser.add_argument("--broker", help="broker URL", type=str, required=True)
        parser.add_argument("--bind", help="URLs to bind to", type=str, required=True, nargs='+')
        parser.add_argument("--peers", help="URLs of all other peers", type=str, required=True, nargs='+')
        args = parser.parse_args()

        super().__init__(peer_id=args.id,
                         broker_url=args.broker,
                         bind_urls=args.bind,
                         peer_urls=args.peers,
                         **kwargs)
        
    def run(self):
        super().run()

