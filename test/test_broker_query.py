#!/usr/bin/env python3

import time
import logging

import pytest

from obci2.core.broker import Broker
from obci2.core.messages import Message
from obci2.core.peer import Peer, PeerInitUrls, TooManyRedirectsException


class QueryTestBroker(Broker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._query_types = {
            'QUERY_1': lambda _: 123,
            'QUERY_2': lambda _: 'abc'
        }
        self._query_redirect_types = {
            'QUERY_REDIRECT': lambda _: 'tcp://127.0.0.1:*',
            'QUERY_REDIRECT_LOOP': lambda _: 'tcp://127.0.0.1:*'
        }


class QueryCommonPeer(Peer):

    def __init__(self, peer_id, urls, **kwargs):
        super().__init__(peer_id, urls, **kwargs)
        self._query_types = {}
        self._query_redirect_types = {}
        self.init_finished = False

    async def initialization_finished(self):
        await super().initialization_finished()
        self.init_finished = True

    async def handle_sync_message(self, msg):
        if msg.type == 'BROKER_QUERY':
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
        else:
            return Message('INVALID_REQUEST', '0', 'Unknown request type')


class QueryAnswerTestPeer(QueryCommonPeer):

    def __init__(self, peer_id, urls, **kwargs):
        super().__init__(peer_id, urls, **kwargs)
        self._query_types = {
            'QUERY_REDIRECT': lambda _: 'kjl',
        }
        self._query_redirect_types = {
            'QUERY_REDIRECT_LOOP': lambda _: 'tcp://127.0.0.1:*'
        }


class QueryAnswerLoopRedirectTestPeer(QueryCommonPeer):

    def __init__(self, peer_id, urls, **kwargs):
        super().__init__(peer_id, urls, **kwargs)
        self._query_types = {}
        self._query_redirect_types = {
            'QUERY_REDIRECT_LOOP': lambda _: 'tcp://127.0.0.1:*'
        }


def run_test(broker_rep,
             broker_xpub,
             broker_xsub,
             peer_pub,
             peer_rep):

    broker = QueryTestBroker([broker_rep], [broker_xpub], [broker_xsub])

    urls = PeerInitUrls(pub_urls=[peer_pub],
                        rep_urls=[peer_rep],
                        broker_rep_url=broker_rep)

    query_peer = QueryCommonPeer(1, urls)
    answer_peer = QueryAnswerTestPeer(2, urls)
    looper_peer = QueryAnswerLoopRedirectTestPeer(3, urls)

    while True:
        if (query_peer.init_finished and
            answer_peer.init_finished and
            looper_peer.init_finished and
                len(broker._peers.keys()) == 4):
            break
        time.sleep(0.05)

    url_query_peer = query_peer._rep_listening_urls[0]
    url_answer_peer = answer_peer._rep_listening_urls[0]
    url_looper_peer = looper_peer._rep_listening_urls[0]

    broker._query_redirect_types['QUERY_REDIRECT'] = lambda _: url_answer_peer
    broker._query_redirect_types['QUERY_REDIRECT_LOOP'] = lambda _: url_answer_peer

    answer_peer._query_redirect_types['QUERY_REDIRECT_LOOP'] = lambda _: url_looper_peer
    looper_peer._query_redirect_types['QUERY_REDIRECT_LOOP'] = lambda _: url_answer_peer

    def do_query_and_check_result(peer, query_type, expected_result):
        future = peer.create_task(peer.query(query_type))
        result = future.result()
        assert result == expected_result

    do_query_and_check_result(query_peer, 'QUERY_1', 123)
    do_query_and_check_result(query_peer, 'QUERY_2', 'abc')
    do_query_and_check_result(query_peer, 'QUERY_REDIRECT', 'kjl')

    with pytest.raises(TooManyRedirectsException):
        do_query_and_check_result(query_peer, 'QUERY_REDIRECT_LOOP', None)

    query_peer.shutdown()
    answer_peer.shutdown()
    looper_peer.shutdown()
    broker.shutdown()


def test_query():
    params = {
        'broker_rep': 'tcp://127.0.0.1:20001',
        'broker_xpub': 'tcp://127.0.0.1:20002',
        'broker_xsub': 'tcp://127.0.0.1:20003',
        'peer_pub': 'tcp://127.0.0.1:*',
        'peer_rep': 'tcp://127.0.0.1:*'
    }
    run_test(**params)
    print('query test finished')

if __name__ == '__main__':
    logging.root.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    logging.root.addHandler(console)

    test_query()
