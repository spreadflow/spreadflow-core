"""
Message formats for IPC.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import pickle
import json

class PickleMessageParser(object):
    """
    Message parser for the pickle stream format.

    Args:
        buffer_max_len (int): The maximum number of bytes buffered while
            parsing a stream of incoming messages. Defaults to 32768.
    """

    MAX_LENGTH = 32768
    HEADER_MAGIC = b'I'[0]

    def __init__(self, buffer_max_len=MAX_LENGTH):
        self._buffer_max_len = buffer_max_len
        self._buffer = b''
        self._header_max_len = len(pickle.dumps(buffer_max_len))

    def push(self, data):
        """
        Push data onto the message parser buffer.

        Args:
            data (bytes): Data as received from the network. Partial messages
            are allowed.

        Raises:
            RuntimeError: If the buffer is full.
        """
        if len(self._buffer) + len(data) > self._buffer_max_len:
            raise RuntimeError('Buffer length exceeded')

        self._buffer += data

    def messages(self):
        """
        Iterate over all available messages.

        Yields:
            object: The next decoded message.
        """
        frame_start = 0

        while self._buffer[frame_start:frame_start + self._header_max_len].find(b'.') > -1:
            if self._buffer[frame_start] is not self.HEADER_MAGIC:
                raise RuntimeError('Frame header is a single integer')

            header = self._buffer[frame_start:frame_start + self._header_max_len]
            header_len = header.index(b'.') + 1

            doc_len = pickle.loads(header[:header_len])
            doc_start = frame_start + header_len
            doc_end = doc_start + doc_len

            if doc_end > len(self._buffer):
                break

            yield pickle.loads(self._buffer[doc_start:doc_end])

            frame_start += header_len + doc_len

        self._buffer = self._buffer[frame_start:]


class PickleMessageBuilder(object):
    """
    Message builder for the pickle stream format.
    """

    def message(self, msg):
        data = pickle.dumps(msg)
        header = pickle.dumps(len(data))
        return header + data

class JsonMessageParser(object):
    """
    Message parser for the JSON lines stream format.

    Args:
        buffer_max_len (int): The maximum number of bytes buffered while
            parsing a stream of incoming messages. Defaults to 32768.
    """

    MAX_LENGTH = 32768

    def __init__(self, buffer_max_len=MAX_LENGTH):
        self._buffer_max_len = buffer_max_len
        self._buffer = b''

    def push(self, data):
        """
        Push data onto the message parser buffer.

        Args:
            data (bytes): Data as received from the network. Partial messages
            are allowed.

        Raises:
            RuntimeError: If the buffer is full.
        """
        if len(self._buffer) + len(data) > self._buffer_max_len:
            raise RuntimeError('Buffer length exceeded')

        self._buffer += data

    def messages(self):
        """
        Iterate over all available messages.

        Yields:
            object: The next decoded message.
        """
        frame_start = 0

        while self._buffer.find(b'\n') > -1:
            frame_len = self._buffer.index(b'\n') + 1
            frame_end = frame_start + frame_len

            if frame_end > len(self._buffer):
                break

            yield json.loads(self._buffer[frame_start:frame_end])

            frame_start += frame_len

        self._buffer = self._buffer[frame_start:]

class JsonMessageBuilder(object):
    """
    Message builder for the JSON lines stream format.
    """

    def message(self, msg):
        return json.dumps(msg)
