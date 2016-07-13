
import asyncio

import numpy as np

from test_peer import TestPeer


class DummySignalSource:
    def __init__(self):
        self._sampling_rate = 512
        self._samples_per_message = 10
        self._channels = 10
        self._dtype = np.float64


    def __aiter__(self):
        return self


    async def __anext__(self):
        data = await self.fetch_data()
        if data:
            return data
        else:
            raise StopAsyncIteration


    async def fetch_data(self):
        await asyncio.sleep(1.0 / (self._sampling_rate / self._samples_per_message))
        return np.random.rand(self._channels,
                              self._samples_per_message).astype(self._dtype)


class TestSenderPeer(TestPeer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._rate = -1
        self._messages_num = -1


    async def send_test_messages(self, msg_type):
        n = 0
        while True:
            await self.send_message(msg_type, '{}'.format(n).encode('ascii'))
            n += 1
            if self._messages_num != -1 and n >= self._messages_num:
                break
            if self._rate != -1:
                await asyncio.sleep(1.0 / self._rate)


    async def amplifier_test(self):
        async for samples in amplifier:
            await self.send_message(0, sig)
        while True:
            # TODO: how to integrate amplifier loop into PyZMQ asyncio loop
            sig = await getSamplesFromAmplifier()
            await self.send_message(0, sig)


    def run(self):
        self._loop.create_task(self.send_test_messages(1))
        self._loop.create_task(self.send_test_messages(2))
        self._loop.create_task(self.send_test_messages(3))
        self._loop.create_task(self.send_test_messages(4))
        super().run()


if __name__ == '__main__':
    peer = TestSenderPeer()
    peer.run()
