from obci2.core.messages import (Message)


class QueryTestBroker(Broker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._query_types = {
            'QUERY_1': 123,
            'QUERY_2': 'abc'
        }
        self._query_redirect_types = {
            'QUERY_REDIRECT': 'tcp://127.0.0.1:20002',
            'QUERY_REDIRECT_LOOP': 'tcp://127.0.0.1:20002'
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

    def do_query_and_wait_for_result(self, query_type):
        pass

    async def handle_sync_message(self, msg):
        await super().handle_sync_message(msg)
        if msg.type == 'BROKER_QUERY':
            query_type = msg.data['type']
            if query_type in self._query_types:
                response = {
                    'type': 'response',
                    'data': self._query_types[query_type]
                }
            elif query_type in self._query_redirect_types:
                response = {
                    'type': 'redirect',
                    'data': self._query_redirect_types[query_type]
                }
            else:
                response = {
                    'type': 'unknown',
                    'data': ''
                }
            return Message('BROKER_QUERY_RESPONSE', '0', response)


class QueryAnswerTestPeer(QueryAnswerTestPeer):

    def __init__(self, peer_id, urls, **kwargs):
        super().__init__(peer_id, urls, **kwargs)
        self._query_types = {
            'QUERY_REDIRECT': 'kjl',
        }
        self._query_redirect_types = {
            'QUERY_REDIRECT_LOOP': 'tcp://127.0.0.1:20003'
        }


class QueryAnswerLoopRedirectTestPeer(QueryAnswerTestPeer):

    def __init__(self, peer_id, urls, **kwargs):
        super().__init__(peer_id, urls, **kwargs)
        self._query_types = {}
        self._query_redirect_types = {
            'QUERY_REDIRECT_LOOP': 'tcp://127.0.0.1:20002'
        }


def test_query_1(broker_rep,
                   broker_xpub,
                   broker_xsub,
                   peer_pub,
                   peer_rep):

    broker = TestBroker([broker_rep], [broker_xpub], [broker_xsub], 1)

    urls = PeerInitUrls(pub_urls=[peer_pub],
                        rep_urls=[peer_rep],
                        broker_rep_url=broker_rep)

    querying_peer = QueryCommonPeer(1, urls)


    while True:
        if peer.init_finished and len(broker._peers.keys()) == 2:
            break
        time.sleep(0.05)

    result = peer.do_query_and_wait_for_result()

    assert result.type == ''
    assert result.data == ''

    querying_peer.shutdown()
    broker.shutdown()


def test_query_1():
    params = {
        'broker_rep': 'tcp://127.0.0.1:20001',
        'broker_xpub': 'tcp://127.0.0.1:20002',
        'broker_xsub': 'tcp://127.0.0.1:20003',
        'peer_pub': 'tcp://127.0.0.1:20004',
        'peer_rep': 'tcp://127.0.0.1:20005'
    }
    run_connection_test(**params)
    print('test_1 finished')


def test_query_2():
    params = {
        'broker_rep': 'tcp://127.0.0.1:20001',
        'broker_xpub': 'tcp://127.0.0.1:20002',
        'broker_xsub': 'tcp://127.0.0.1:20003',
        'peer_pub': 'tcp://127.0.0.1:*',
        'peer_rep': 'tcp://127.0.0.1:*'
    }
    run_connection_test(**params)
    print('test_2 finished')


if __name__ == '__main__':
    test_1()
    test_2()
