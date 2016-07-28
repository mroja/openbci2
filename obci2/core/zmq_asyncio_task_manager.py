import zmq
import zmq.asyncio

from .asyncio_task_manager import AsyncioTaskManager


class ZmqAsyncioTaskManager(AsyncioTaskManager):
    @staticmethod
    def new_event_loop():
        return zmq.asyncio.ZMQEventLoop()

    _thread_name = 'ZmqAsyncioTaskManager'
    _logger_name = 'ZmqAsyncioTaskManager'

    def __init__(self, asyncio_loop=None, zmq_context=None, zmq_io_threads=1):
        assert zmq_context is None or isinstance(zmq_context, zmq.asyncio.Context)
        super().__init__(asyncio_loop)
        self._zmq_io_threads = zmq_io_threads
        if zmq_context is None:
            self._owns_ctx = True
            self._ctx = zmq.asyncio.Context(io_threads=self._zmq_io_threads)
        else:
            self._owns_ctx = False
            self._ctx = zmq_context

    @property
    def owns_zmq_context(self):
        return self._owns_ctx

    def _cleanup(self):
        if self._owns_ctx:
            # destroy requires there are no active sockets in other threads
            # TODO: is this condition true?
            self._ctx.destroy(linger=0)
            self._ctx = None
        super()._cleanup()
