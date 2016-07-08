
import time
import random

import pytest

from obci2.core.peer import Peer, PeerInitUrls
from obci2.core.broker import Broker


class TestBroker(Broker):
    pass

class TestPeer(Peer):
    def __init__(self, peer_id, urls, **kwargs):
        super().__init__(peer_id, urls, **kwargs)
        self.init_finished = False
        
    async def initialization_finished(self):
        await super().initialization_finished()
        self.init_finished = True


class SingleMessageSenderTestPeer(TestPeer):
    pass


@pytest.mark.timeout(10)
def run_connection_test(broker_rep, 
                        broker_xpub,
                        broker_xsub,
                        peer_pub,
                        peer_rep):

    #broker = TestBroker([broker_rep], [broker_xpub], [broker_xsub])
    
    urls = PeerInitUrls(pub_urls=[peer_pub], 
                        rep_urls=[peer_rep],
                        broker_rep_url=broker_rep)    
    peer = TestPeer(1, urls)

    #while True:
    #    if peer.init_finished and len(broker._peers.keys()) == 2:
    #        break
    #    time.sleep(0.1)
    #    print('x')
        
    #broker.shutdown()
    peer.shutdown()


def test_connection_1():
    params = {
        'broker_rep': 'tcp://127.0.0.1:20001',
        'broker_xpub': 'tcp://127.0.0.1:20002',
        'broker_xsub': 'tcp://127.0.0.1:20003',
        'peer_pub': 'tcp://127.0.0.1:20004',
        'peer_rep': 'tcp://127.0.0.1:20005'
    }
    run_connection_test(**params)
    
    
def test_connection_2():
    params = {
        'broker_rep': 'tcp://127.0.0.1:20001',
        'broker_xpub': 'tcp://127.0.0.1:20002',
        'broker_xsub': 'tcp://127.0.0.1:20003',
        'peer_pub': 'tcp://127.0.0.1:*',
        'peer_rep': 'tcp://127.0.0.1:*'
    }
    run_connection_test(**params)
    
    
def _test_connection_3():
    peer_pub_urls = [
        'tcp://127.0.0.1:20000', 'tcp://127.0.0.1:20001',
        'tcp://127.0.0.1:20002', 'tcp://127.0.0.1:20003'
    ]
    peer_rep_urls = [
        'tcp://127.0.0.1:30000', 'tcp://127.0.0.1:30001',
        'tcp://127.0.0.1:30002', 'tcp://127.0.0.1:30003'
    ]
    
    peer = Peer()

    time.sleep(0.5)


def _test_connection_4():
    broker_rep = 'tcp://127.0.0.1:20001'
    broker_xpub = 'tcp://127.0.0.1:20002'
    broker_xsub = 'tcp://127.0.0.1:20003'
    
    peer_pub = 'tcp://127.0.0.1:*'
    peer_rep = 'tcp://127.0.0.1:*'
    
    broker = TestBroker()

    A_msgs_count = 25
    B_msgs_count = 25
    
    msg_to_send = random.shuffle(A_msgs_count * ['A'] + B_msgs_count * ['B'])

    id_counter = 1

    peers_receive_A = []
    for i in range(A_msgs_count):
        peers_receive_A.append(SingleMessageReceiverTestPeer(id_counter, 'A'))
        id_counter += 1

    peers_receive_B = []
    for i in range(B_msgs_count):
        peers_receive_B.append(SingleMessageReceiverTestPeer(id_counter, 'B'))
        id_counter += 1

    peers_senders = []
    for i in range(A_msgs_count + B_msgs_count):
        peers_senders.append(SingleMessageSenderTestPeer(id_counter, msg_to_send.pop()))
        id_counter += 1

    time.sleep(1.0)

    for peer in peers_receive_A:
        peer.shutdown()
    for peer in peers_receive_B:
        peer.shutdown()
    for peer in peers_senders:
        peer.shutdown()

    broker.shutdown()

    for peer in peers_receive_A:
        assert(peer.received_messages_count == 1)

    for peer in peers_receive_B:
        assert(peer.received_messages_count == 1)

    for peer in peers_senders:
        assert(peer.sent_messages_count == 1)
        
if __name__=='__main__':
    import threading
    test_connection_1()
    test_connection_2()
    print(threading.enumerate())