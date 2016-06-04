# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Integration tests for spreadflow twistd application runner.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import errno
import fcntl
import fixtures
import os
import shutil
import subprocess
import threading
import time
import unittest

try:
    import queue
except ImportError:
    import Queue as queue

MAXWAIT = 5.0
FIXTURE_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures')

class _TimeoutError(Exception):
    pass

class _StreamReader(object):
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
                raise _TimeoutError()

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

class SpreadflowTwistdIntegrationTestCase(unittest.TestCase):
    """
    Integration tests for spreadflow twistd application runner.
    """

    longMessage = True

    def _format_stream(self, stdout, stderr):
        return '\nSTDOUT:\n{0}\nSTDERR:\n{1}'.format(
            stdout.decode('utf-8') or '*** EMPTY ***', stderr.decode('utf-8') or '*** EMPTY ***')

    def test_oneshot(self):
        """
        Process should exit with a zero result.
        """

        config = os.path.join(FIXTURE_DIRECTORY, 'spreadflow-simple.conf')
        with fixtures.TempDir() as fix:
            rundir = fix.path
            argv = ['-n', '-d', rundir, '-c', config, '-o']
            proc = subprocess.Popen(['spreadflow-twistd'] + argv,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            stdout_value, stderr_value = proc.communicate()
            self.assertEqual(proc.returncode, 0, self._format_stream(stdout_value, stderr_value))

    def test_exit_on_failure(self):
        """
        Process should exit with a non-zero result as soon as a process fails.
        """

        config = os.path.join(FIXTURE_DIRECTORY, 'spreadflow-fail-proc.conf')
        with fixtures.TempDir() as fix:
            rundir = fix.path
            argv = ['-n', '-d', rundir, '-c', config]
            proc = subprocess.Popen(['spreadflow-twistd'] + argv,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            stdout_value, stderr_value = proc.communicate()
            self.assertEqual(proc.returncode, 1, self._format_stream(stdout_value, stderr_value))

    def test_subprocess_worker_producer(self):
        """
        Worker process reads messages from stdin and writes results to stdout.
        """

        logger = 'spreadflow_core.scripts.spreadflow_twistd.StderrLogger'
        config = os.path.join(FIXTURE_DIRECTORY, 'spreadflow-partitions.conf')
        with fixtures.TempDir() as fix:

            rundir = fix.path
            pidfile = os.path.join(rundir, 'twistd.pid')
            argv = ['-n', '-d', rundir, '-c', config, '--logger', logger]
            argv += ['--pidfile', pidfile, '--multiprocess', '--partition']
            argv += ['producer']
            proc = subprocess.Popen(['spreadflow-twistd'] + argv,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)

            stream_data = {proc.stdout: b'', proc.stderr: b''}

            reader = _StreamReader([proc.stdout, proc.stderr])
            reader.start()

            for stream, data in reader.drain():
                stream_data[stream] += data
                if stream_data[proc.stdout]:
                    break
            else:
                self.fail('Worker process is expected to emit a message to stdout{0}'.format(self._format_stream(stream_data[proc.stdout], stream_data[proc.stderr])))

            # Close stdin, this signals the worker process to terminate.
            proc.stdin.close()

            proc.wait()
            self.assertEqual(proc.returncode, 0, self._format_stream(stream_data[proc.stdout], stream_data[proc.stderr]))

            for stream, data in reader.drain(0):
                stream_data[stream] += data

            reader.join()

    def test_subprocess_worker_consumer(self):
        """
        Worker process reads messages from stdin and writes results to stdout.
        """

        logger = 'spreadflow_core.scripts.spreadflow_twistd.StderrLogger'
        config = os.path.join(FIXTURE_DIRECTORY, 'spreadflow-partitions.conf')
        with fixtures.TempDir() as fix:

            rundir = fix.path
            pidfile = os.path.join(rundir, 'twistd.pid')
            argv = ['-n', '-d', rundir, '-c', config, '--logger', logger]
            argv += ['--pidfile', pidfile, '--multiprocess', '--partition']
            argv += ['consumer']
            proc = subprocess.Popen(['spreadflow-twistd'] + argv,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)

            stream_data = {proc.stdout: b'', proc.stderr: b''}

            reader = _StreamReader([proc.stdout, proc.stderr])
            reader.start()

            msg = b"I51\n.(dp0\nS'item'\np1\nS'hello world'\np2\nsS'port'\np3\nI0\ns."
            proc.stdin.write(msg)
            proc.stdin.flush()

            marker = b'[spreadflow_core.proc.DebugLog#debug] ' \
                b'Item received: hello world'
            for stream, data in reader.drain():
                stream_data[stream] += data
                if marker in stream_data[proc.stderr]:
                    break
            else:
                self.fail('Worker process is expected to emit a message to stderr{0}'.format(self._format_stream("*** HIDDEN ***", stream_data[proc.stderr])))

            # Close stdin, this signals the worker process to terminate.
            proc.stdin.close()

            proc.wait()
            self.assertEqual(proc.returncode, 0, self._format_stream(stream_data[proc.stdout], stream_data[proc.stderr]))

            for stream, data in reader.drain(0):
                stream_data[stream] += data

            reader.join()

    def test_subprocess_controller(self):
        """
        Controller process runs two workers and collects their output.
        """

        logger = 'spreadflow_core.scripts.spreadflow_twistd.StderrLogger'
        config = os.path.join(FIXTURE_DIRECTORY, 'spreadflow-partitions.conf')
        with fixtures.TempDir() as fix:
            rundir = fix.path
            # FIXME: subprocess controller currently does not propagate the
            # configuration file path to its child processess.
            shutil.copy(config, os.path.join(rundir, 'spreadflow.conf'))
            pidfile = os.path.join(rundir, 'twistd.pid')
            argv = ['-n', '-d', rundir, '--logger', logger]
            argv += ['--pidfile', pidfile, '--multiprocess']
            proc = subprocess.Popen(['spreadflow-twistd'] + argv,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)

            stream_data = {proc.stdout: b'', proc.stderr: b''}

            reader = _StreamReader([proc.stdout, proc.stderr])
            reader.start()

            marker = b'[spreadflow_core.proc.DebugLog#debug] ' \
                b'Item received: hello world'
            for stream, data in reader.drain():
                stream_data[stream] += data
                if marker in stream_data[proc.stderr]:
                    break
            else:
                self.fail('Worker process is expected to emit a message to stderr{0}'.format(self._format_stream(stream_data[proc.stdout], stream_data[proc.stderr])))

            # Send SIGTERM to controller.
            proc.terminate()

            for stream, data in reader.drain(0):
                stream_data[stream] += data

            proc.wait()
            self.assertEqual(proc.returncode, 0, self._format_stream(stream_data[proc.stdout], stream_data[proc.stderr]))

            for stream, data in reader.drain(0):
                stream_data[stream] += data

            reader.join()
