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
    HEADER_MAX_LEN = 24

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

        while self._buffer.find(b'.', frame_start, frame_start + self.HEADER_MAX_LEN) > -1:
            header = self._buffer[frame_start:frame_start + self.HEADER_MAX_LEN]
            header_len = header.index(b'.') + 1

            doc_len = pickle.loads(header[:header_len])
            if not isinstance(doc_len, int) or doc_len < 0:
                raise ValueError('Document length must be a positive integer')

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

    def __init__(self, protocol=2):
        self.protocol = protocol

    def message(self, msg):
        data = pickle.dumps(msg, protocol=self.protocol)
        header = pickle.dumps(len(data), protocol=self.protocol)
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

            yield json.loads(self._buffer[frame_start:frame_end].decode('utf-8'))

            frame_start += frame_len

        self._buffer = self._buffer[frame_start:]

class JsonMessageBuilder(object):
    """
    Message builder for the JSON lines stream format.
    """

    def message(self, msg):
        return json.dumps(msg).encode('utf-8') + b'\n'
