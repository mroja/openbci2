
import asyncio
import logging
import threading


class AsyncioTaskManager:
    @staticmethod
    def new_event_loop():
        return asyncio.new_event_loop()

    _thread_name = 'AsyncioTaskManager'
    _logger_name = 'AsyncioTaskManager'

    def __init__(self, asyncio_loop=None):
        assert asyncio_loop is None or isinstance(asyncio_loop, asyncio.BaseEventLoop)
        self._logger = logging.getLogger(self._logger_name)
        self._tasks = []
        self._shutdown_lock = threading.Lock()
        self._cancel_tasks_finished = threading.Condition(threading.Lock())
        self._create_task_enabled = True
        if asyncio_loop is not None:
            self._owns_loop = False
            self._loop = asyncio_loop
            self._thread = None
        else:
            self._owns_loop = True
            self._loop = self.new_event_loop()
            self._thread = threading.Thread(target=self.__thread_func,
                                            name=self._thread_name)
            self._thread.daemon = True
            self._thread.start()

    def __del__(self):
        # When running with 'self._owns_loop == True'
        # this function requires 'self._thread.daemon == True',
        # otherwise thread will not end properly when program ends
        # and this function won't be called by Python.
        self.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    @property
    def owns_asyncio_loop(self):
        return self._owns_loop

    def create_task(self, coro):
        """ Can be called from any thread. """
        assert self._loop is not None

        if not self._create_task_enabled:
            raise Exception('AsyncioTaskManager is shutting down. Creating new tasks disabled.')

        future = None
        async def coro_wrapper():
            nonlocal future
            nonlocal coro
            fut = None
            try:
                fut = asyncio.ensure_future(coro, loop=self._loop)
                return await fut
            except asyncio.CancelledError:
                self._logger.warn('Coroutine cancelled: {}, {}'.format(fut, coro))
                raise
            except Exception:
                self._logger.exception('Exception in coroutine: {}, {}'.format(fut, coro))
                raise
            finally:
                if future in self._tasks:
                    self._tasks.remove(future)

        if asyncio.get_event_loop() == self._loop:
            future = asyncio.ensure_future(coro_wrapper(), loop=self._loop)
        else:
            future = asyncio.run_coroutine_threadsafe(coro_wrapper(), loop=self._loop)
        self._tasks.append(future)
        return future

    def shutdown(self):
        """
        This function can be called from ANY thread.
        It will block until all pending tasks are finished.
        Can be called multiple times.
        """
        with self._shutdown_lock:
            if not self._create_task_enabled:
                assert len(self._tasks) == 0
                return
            self._create_task_enabled = False
            if self._owns_loop:
                if self._thread.is_alive() and self._loop.is_running:
                    self._loop.call_soon_threadsafe(self._loop.stop)
                self._thread.join()
            else:
                if self._loop.is_running:
                    self._loop.run_coroutine_threadsafe(self.__cancel_all_tasks())
                    self._cancel_tasks_finished.wait()
            assert len(self._tasks) == 0

    def _cleanup(self):
        """
        Can be reimplemented to perform extra cleanup tasks.
        Subclasses must call `super()._cleanup()`.
        """
        pass

    async def __cancel_all_tasks(self):
        try:
            gather_future = asyncio.gather(*self._tasks)
            gather_future.cancel()
            await asyncio.wait_for(gather_future, None, loop=self._loop)
        finally:
            try:
                self._cleanup()
            finally:
                self._cancel_tasks_finished.notify_all()

    def __thread_func(self):
        assert self._owns_loop and self._loop is not None and self._thread is not None
        try:
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug("Setting message loop for thread '{}' ({})."
                                   .format(self._thread.name, self._thread.ident))
            asyncio.set_event_loop(self._loop)
            self._logger.debug('Starting message loop...')
            self._loop.run_forever()
        except Exception:
            self._logger.exception('Exception in asyncio event loop:')
            raise
        finally:
            self._create_task_enabled = False
            self._logger.debug('Message loop stopped. Cancelling all pending tasks.')
            try:
                tasks = asyncio.gather(*asyncio.Task.all_tasks())
                tasks.cancel()
                try:
                    self._loop.run_until_complete(tasks)
                except asyncio.CancelledError:
                    # self._logger.info('cancelled: {}'.format(tasks), exc_info=True)
                    pass
                except Exception:
                    self._logger.exception('Exception during message loop shutdown phase:')
                    raise
                finally:
                    self._loop.close()
            except Exception:
                self._logger.exception('Unknown exception during message loop shutdown phase:')
                raise
            finally:
                self._cleanup()
                self._logger.debug('All cleanup tasks finished. Event loop thread will end now.')
