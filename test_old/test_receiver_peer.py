
import time
import asyncio

from test_peer import TestPeer


class TestReceiverPeer(TestPeer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._validate_data = True
        self._last_val = None

        self._work_duration = 0.1

        self._log_messages = False

        self.set_filter(self._id.to_bytes(1, byteorder='little'))

    async def handle_message(self, msg_type, sending_peer, msg_data):
        await super().handle_message(msg_type, sending_peer, msg_data)

        if self._validate_data:
            val = int(msg_data.decode('ascii'))
            if self._last_val is not None:
                x = val - self._last_val
                if x != 1:
                    print('Transmission error ({} - {} = {}, should be 1)'
                          .format(val, self._last_val, x))
            self._last_val = val

        # simulate some work
        if self._work_duration != -1:
            time.sleep(self._work_duration)


if __name__ == '__main__':
    peer = TestReceiverPeer()
    peer.run()

