
import asyncio
from test_peer import TestPeer


class TestReceiverPeer(TestPeer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def handle_message(self, msg_type, sending_peer, msg_data):
        await super().handle_message(msg_type, sending_peer, msg_data)


if __name__ == '__main__':
    peer = TestReceiverPeer()
    peer.run()

