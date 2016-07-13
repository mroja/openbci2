
import time


class MsgPerfStats:

    def __init__(self, interval, name=''):
        self._name = name
        self._interval = interval
        self._calc_size = False

    def reset(self):
        self._last_time = time.time()
        self._start_time = self._last_time
        self._count = 0
        self._total_size = 0

    def msg(self, msg):
        self._last_time = time.time()
        self._count += 1
        if self._calc_size:
            self._total_size += sum(map(len, msg.serialize()))
        measurement_time = self._last_time - self._start_time

        if measurement_time > self._interval:
            if self._calc_size:
                mean_size = int(self._total_size / self._count)
                megabytes_per_second = (self._total_size / measurement_time) / 1e6
            messages_per_second = self._count / measurement_time

            if self._name:
                print('stats for "{}"'.format(self._name))
            print('message count:     {:6d} [msgs]'.format(self._count))
            if self._calc_size:
                print('mean message size: {:6d} [B]'.format(mean_size))
            print('mean throughput:   {:4.2f} [msg/s]'.format(messages_per_second))
            if self._calc_size:
                print('mean throughput:   {:2.4f} [MB/s]'.format(megabytes_per_second))
            print('measurement time:  {:2.4f} [s]'.format(measurement_time))
            print('')

            self.reset()

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def interval(self, interval):
        self._interval = interval
        self.reset()
