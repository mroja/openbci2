
import time
import asyncio

import zmq


def bind_to_urls(socket, urls):
    listening_urls = []
    for url in urls:
        socket.bind(url)
        real_url = socket.getsockopt(zmq.LAST_ENDPOINT)
        if real_url:
            listening_urls.append(real_url.decode())
    return listening_urls


class TimeoutException(Exception):
    pass

async def recv_multipart_with_timeout(socket, timeout=1.0, sleep_interval=0.01):
    start_time = time.monotonic()
    while True:
        try:
            response = await socket.recv_multipart(zmq.NOBLOCK)
            return response
        except zmq.error.Again:
            if time.monotonic() - start_time > timeout:
                raise TimeoutException()
            await asyncio.sleep(sleep_interval)
