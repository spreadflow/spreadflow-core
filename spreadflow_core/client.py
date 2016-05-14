from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import struct

from bson import BSON
from twisted.internet import defer, protocol
from twisted.internet.endpoints import clientFromString

class MessageParserMissingError(Exception):
    pass

class MessageParser(object):

    MAX_LENGTH = 32768


    def __init__(self):
        self._buffer = b''


    def push(self, data):
        if len(self._buffer) + len(data) > self.MAX_LENGTH:
            raise RuntimeError('Buffer length exceeded')

        self._buffer += data


    def messages(self):
        doc_start = 0

        while doc_start + 4 < len(self._buffer):
            # http://bsonspec.org/spec.html
            (doc_len, ) = struct.unpack(b'<l', self._buffer[doc_start:doc_start + 4])
            if (doc_start + doc_len > len(self._buffer)):
                break

            doc = BSON(self._buffer[doc_start:doc_start + doc_len])
            yield doc.decode()

            doc_start += doc_len

        self._buffer = self._buffer[doc_start:]


class SchedulerClientProtocol(protocol.Protocol):
    """A client protocol suitable to control a remote scheduler.
    """

    # pylint: disable=invalid-name

    def __init__(self):
        self._parser = None
        self._stopped = None

    def loseConnection(self):
        """
        Stop the remote scheduler and disconnect the transport.

        Returns
            defer.Deferred: Fires when the connection has been closed.
        """
        if self.connected and not self._stopped:
            self._stopped = defer.Deferred()
            self.transport.loseConnection()

        return self._stopped

    def dataReceived(self, data):
        self._parser.push(data)
        for msg in self._parser.messages():
            self.factory.dispatch(msg['port'], msg['item']) # pylint: disable=no-member

    def connectionMade(self):
        self._parser = self.factory.parser_factory() # pylint: disable=no-member
        self._stopped = None

    def connectionLost(self, reason=protocol.connectionDone):
        self.connected = 0
        self._parser = None
        if self._stopped:
            self._stopped.callback(self)
            self._stopped = None
        else:
            reason.raiseException()


class SchedulerClientFactory(protocol.ClientFactory):

    protocol = SchedulerClientProtocol

    def __init__(self, portmap, scheduler, parser_factory):
        self.parser_factory = parser_factory
        self.portmap = portmap
        self.scheduler = scheduler

    def dispatch(self, port, item):
        self.scheduler.send(item, self.portmap[port])


class SchedulerClient(object):
    endpoint = None
    factory_class = SchedulerClientFactory
    parser_factory = None
    portmap = None
    reactor = None
    scheduler = None
    _protocol = None

    def attach(self, scheduler, reactor):
        self.scheduler = scheduler
        self.reactor = reactor

    @defer.inlineCallbacks
    def start(self):
        assert self.parser_factory is not None, "Subclass must specify a parser_factory"
        factory = self.factory_class(self.portmap, self.scheduler, self.parser_factory)
        client = clientFromString(self.reactor, self.endpoint)
        self._protocol = yield client.connect(factory)

    @defer.inlineCallbacks
    def join(self):
        if self._protocol:
            yield self._protocol.loseConnection()

    def detach(self):
        self.scheduler = None
        self.reactor = None
