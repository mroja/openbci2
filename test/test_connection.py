#!/usr/bin/env python3

import asyncio
import random
import time
import logging

import pytest
import zmq

from obci2.core.broker import Broker
from obci2.core.messages import Message, NullMessageSerializer
from obci2.core.peer import Peer, PeerInitUrls


class CustomBroker(Broker):
    pass


class CustomPeer(Peer):

    def __init__(self, peer_id, urls, **kwargs):
        super().__init__(peer_id, urls, **kwargs)
        self.init_finished = False

    async def initialization_finished(self):
        await super().initialization_finished()
        self.init_finished = True


class CustomBrokerWithMsgHistory(CustomPeer):

    def __init__(self, peer_id, urls, **kwargs):
        super().__init__(peer_id, urls, **kwargs)
        self.received_sync_messages = []
        self.received_async_messages = []

    async def handle_sync_message(self, msg):
        await super().handle_sync_message(msg)
        self.received_sync_messages.append(msg)
        if msg.type == 'TEST':
            return Message('TEST_RESPONSE', self._id)
        else:
            return Message('INVALID_REQUEST', self._id, 'message type not recognized')

    async def handle_async_message(self, msg):
        await super().handle_async_message(msg)
        self.received_async_messages.append(msg)


class SingleMessageSenderTestPeer(CustomPeer):

    def __init__(self, peer_id, urls, msg_to_send, messages_count=1, **kwargs):
        super().__init__(peer_id, urls, **kwargs)
        self.msg_to_send = msg_to_send
        self.messages_count = messages_count
        self.sent_messages_count = 0

    async def send_messages(self):
        for _ in range(self.messages_count):
            await self.send_message(Message(self.msg_to_send, self._id))
            self.sent_messages_count += 1


class SingleMessageReceiverTestPeer(CustomPeer):

    def __init__(self, peer_id, urls, msg_to_receive, **kwargs):
        super().__init__(peer_id, urls, **kwargs)
        self.msg_to_receive = msg_to_receive
        self.received_messages_count = 0

    async def initialization_finished(self):
        self.set_filter(self.msg_to_receive)
        await super().initialization_finished()

    async def handle_async_message(self, msg):
        await super().handle_async_message(msg)
        if msg.type == self.msg_to_receive:
            self.received_messages_count += 1


Message.register_serializer('A', NullMessageSerializer)
Message.register_serializer('B', NullMessageSerializer)
Message.register_serializer('TEST', NullMessageSerializer)
Message.register_serializer('TEST_RESPONSE', NullMessageSerializer)


@pytest.mark.timeout(10)
def run_connection_test(broker_rep,
                        broker_xpub,
                        broker_xsub,
                        peer_pub,
                        peer_rep):

    broker = CustomBroker([broker_rep], [broker_xpub], [broker_xsub])

    urls = PeerInitUrls(pub_urls=[peer_pub],
                        rep_urls=[peer_rep],
                        broker_rep_url=broker_rep)
    peer = CustomPeer(1, urls)

    while True:
        if peer.init_finished and len(broker._peers.keys()) == 2:
            break
        time.sleep(0.05)

    peer.shutdown()
    broker.shutdown()


@pytest.mark.timeout(10)
def run_connection_test_2(broker_rep,
                          broker_xpub,
                          broker_xsub,
                          peer_pub,
                          peer_rep):

    broker = CustomBroker([broker_rep], [broker_xpub], [broker_xsub])

    urls = PeerInitUrls(pub_urls=[peer_pub],
                        rep_urls=[peer_rep],
                        broker_rep_url=broker_rep)
    peer1 = CustomPeer(1, urls)
    peer2 = CustomPeer(2, urls)

    while True:
        if peer1.init_finished and peer2.init_finished and len(broker._peers.keys()) == 3:
            break
        time.sleep(0.05)

    peer1.shutdown()
    peer2.shutdown()
    broker.shutdown()


def test_connection_1():
    params = {
        'broker_rep': 'tcp://127.0.0.1:20001',
        'broker_xpub': 'tcp://127.0.0.1:20002',
        'broker_xsub': 'tcp://127.0.0.1:20003',
        'peer_pub': 'tcp://127.0.0.1:20004',
        'peer_rep': 'tcp://127.0.0.1:20005'
    }
    run_connection_test(**params)
    print('test_1 finished')


def test_connection_2a():
    params = {
        'broker_rep': 'tcp://127.0.0.1:20001',
        'broker_xpub': 'tcp://127.0.0.1:20002',
        'broker_xsub': 'tcp://127.0.0.1:20003',
        'peer_pub': 'tcp://127.0.0.1:*',
        'peer_rep': 'tcp://127.0.0.1:*'
    }
    run_connection_test(**params)
    print('test_2a finished')


def test_connection_2b():
    params = {
        'broker_rep': 'tcp://127.0.0.1:20001',
        'broker_xpub': 'tcp://127.0.0.1:20002',
        'broker_xsub': 'tcp://127.0.0.1:20003',
        'peer_pub': 'tcp://127.0.0.1:*',
        'peer_rep': 'tcp://127.0.0.1:*'
    }
    run_connection_test_2(**params)
    print('test_2b finished')


def test_connection_3():
    broker_rep = 'tcp://127.0.0.1:20001'
    broker_xpub = 'tcp://127.0.0.1:20002'
    broker_xsub = 'tcp://127.0.0.1:20003'

    peer_pub_urls = [
        'tcp://127.0.0.1:20100', 'tcp://127.0.0.1:20101',
        'tcp://127.0.0.1:20102', 'tcp://127.0.0.1:20103'
    ]
    peer_rep_urls = [
        'tcp://127.0.0.1:20200', 'tcp://127.0.0.1:30201',
        'tcp://127.0.0.1:20202', 'tcp://127.0.0.1:30203'
    ]

    broker = CustomBroker([broker_rep], [broker_xpub], [broker_xsub])

    urls = PeerInitUrls(pub_urls=peer_pub_urls,
                        rep_urls=peer_rep_urls,
                        broker_rep_url=broker_rep)
    peer = CustomBrokerWithMsgHistory(1, urls)

    while True:
        if peer.init_finished and len(broker._peers.keys()) == 2:
            break
        time.sleep(0.05)

    ctx = zmq.Context()

    sub_sockets = [ctx.socket(zmq.SUB) for _ in range(len(peer_pub_urls))]
    req_sockets = [ctx.socket(zmq.REQ) for _ in range(len(peer_rep_urls))]

    # test async

    for url, sub in zip(peer_pub_urls, sub_sockets):
        sub.connect(url)
        sub.subscribe(b'')

    async def send_test_messages():
        for _ in range(1):
            await peer.send_message(Message('TEST', '1'))
            await asyncio.sleep(0.1)

    time.sleep(0.5)

    peer.create_task(send_test_messages())

    time.sleep(0.5)

    for url, sub in zip(peer_pub_urls, sub_sockets):
        msg = sub.recv_multipart()
        msg = Message.deserialize(msg)
        assert msg.type == 'TEST'
        sub.disconnect(url)

    # test sync

    for url, req in zip(peer_rep_urls, req_sockets):
        req.connect(url)
        req.send_multipart(Message('TEST', '1').serialize())
        replay = req.recv_multipart()
        msg = Message.deserialize(replay)
        assert msg.type == 'TEST_RESPONSE'
        req.disconnect(url)

    # shutdown

    for sub in sub_sockets:
        sub.close(linger=0)
    for req in req_sockets:
        req.close(linger=0)
    ctx.destroy()

    peer.shutdown()
    broker.shutdown()

    print('test_3 finished')


def test_connection_4():
    broker_rep = 'tcp://127.0.0.1:20001'
    broker_xpub = 'tcp://127.0.0.1:20002'
    broker_xsub = 'tcp://127.0.0.1:20003'

    peer_pub = 'tcp://127.0.0.1:*'
    peer_rep = 'tcp://127.0.0.1:*'

    A_msgs_count = 5
    B_msgs_count = 5

    broker = CustomBroker([broker_rep], [broker_xpub], [broker_xsub])

    msg_to_send = A_msgs_count * ['A'] + B_msgs_count * ['B']
    random.shuffle(msg_to_send)

    id_counter = 1

    urls = PeerInitUrls(pub_urls=[peer_pub],
                        rep_urls=[peer_rep],
                        broker_rep_url=broker_rep)

    time.sleep(1.0)

    peers_receive_A = []
    for _ in range(A_msgs_count):
        peers_receive_A.append(SingleMessageReceiverTestPeer(id_counter, urls, msg_to_receive='A'))
        id_counter += 1

    peers_receive_B = []
    for _ in range(B_msgs_count):
        peers_receive_B.append(SingleMessageReceiverTestPeer(id_counter, urls, msg_to_receive='B'))
        id_counter += 1

    peers_senders = []
    for msg in msg_to_send:
        peers_senders.append(SingleMessageSenderTestPeer(id_counter, urls, msg_to_send=msg))
        id_counter += 1

    all_peers = peers_receive_A + peers_receive_B + peers_senders

    while True:
        all_ready = True
        for peer in all_peers:
            if not peer.init_finished:
                all_ready = False
                break
        if all_ready:
            break
        time.sleep(0.05)

    time.sleep(1.0)

    for peer in peers_senders:
        peer.create_task(peer.send_messages())

    time.sleep(1.0)

    for peer in all_peers:
        peer.shutdown()

    broker.shutdown()

    for peer in peers_senders:
        assert(peer.sent_messages_count == 1)

    for peer in peers_receive_A:
        # print(peer.received_messages_count)
        assert(peer.received_messages_count == A_msgs_count)

    for peer in peers_receive_B:
        # print(peer.received_messages_count)
        assert(peer.received_messages_count == B_msgs_count)

    print('test_4 finished')

if __name__ == '__main__':
    # logging.root.setLevel(logging.DEBUG)
    logging.root.setLevel(logging.INFO)
    console = logging.StreamHandler()
    logging.root.addHandler(console)

    test_connection_1()
    test_connection_2a()
    test_connection_2b()
    test_connection_3()
    test_connection_4()
