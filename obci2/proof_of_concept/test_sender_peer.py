
import asyncio
from test_peer import TestPeer


class TestSenderPeer(TestPeer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def send_test_messages(self):
        n = 0
        while True:
            await self.send_message(0, '{}'.format(n).encode('utf-8'))
            n += 1
            if n >= 1000:
                break
            await asyncio.sleep(1.0)

    # NOTE: this is mockup code
    async def amplifier_test(self):
        while True:
            # TODO: how to integrate amplifier loop into PyZMQ asyncio loop
            sig = await getSamplesFromAmplifier()
            await self.send_message(0, sig)

    def run(self):
        self._loop.create_task(self.send_test_messages())
        super().run()


if __name__ == '__main__':
    peer = TestSenderPeer()
    peer.run()

