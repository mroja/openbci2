
import asyncio

import zmq

from basic_peer import BasicPeer


class Peer(BasicPeer):
    def __init__(self, broker_url, **kwargs):
        super().__init__(**kwargs)
        self._broker_url = broker_url

        # TODO: communication channel with broker
        self._broker = self._ctx.socket(zmq.REQ)
        self._broker.connect(self._broker_url)
        
        self._heartbeat_delay = 0.5
        
        if self._log_peers_info:
            print("Broker URL: {}\n".format(self._broker_url))


    async def send_system_msg(self, msg):
        msg = [self._id.to_bytes(2, byteorder='little'), msg]
        await self._broker.send_multipart(msg)


    async def heartbeat(self):
        while True:
            await self.send_system_msg(b'HEARTBEAT')
            await asyncio.sleep(self._heartbeat_delay)
            if not self._running:
                break

    
    def run(self):
        self._loop.create_task(self.heartbeat())
        super().run()

