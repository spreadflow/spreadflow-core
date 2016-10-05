# -*- coding: utf-8 -*-

"""
Test helpers.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import errno
import fcntl
import os
import threading
import time

try:
    import queue
except ImportError:
    import Queue as queue

MAXWAIT = 5.0

class StreamTimeoutError(Exception):
    """
    Timeout occured during drain().
    """
    pass

class StreamsReader(object):
    """
    A non-blocking reader for multiple input streams.
    """
    def __init__(self, streams):
        self._queue = queue.Queue()
        self._streams = streams
        self._threads = None

    def drain(self, timeout=MAXWAIT):
        if timeout > 0:
            deadline = time.time() + timeout

        while True:
            if timeout > 0 and time.time() > deadline:
                raise StreamTimeoutError()

            try:
                stream, data = self._queue.get(timeout=timeout)
            except queue.Empty:
                break
            else:
                yield stream, data

    def start(self):
        for stream in self._streams:
            thread = threading.Thread(target=self._reader, args=[stream])
            thread.daemon = True
            thread.start()

    def join(self):
        if self._threads is not None:
            for thread in self._threads:
                thread.join()

    def _reader(self, stream):
        """
        Performs non-blocking read from a stream. Use it in a thread.
        """
        flags = fcntl.fcntl(stream, fcntl.F_GETFL)
        fcntl.fcntl(stream, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        while True:
            try:
                data = stream.read()
            except (OSError, IOError) as e:
                if e.errno == errno.EINTR or e.errno == errno.EAGAIN:
                    continue
                raise
            else:
                if data is not None:
                    self._queue.put((stream, data))
                    if data == b'':
                        break
