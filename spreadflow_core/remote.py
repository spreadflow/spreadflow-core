"""
Protocol and protocol factories allowing communication with remote instances.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import re

from twisted.internet import defer, protocol
from twisted.internet import interfaces
from twisted.internet.endpoints import clientFromString, serverFromString
from zope.interface import implementer


class MessageHandler(object):
    """
    A simple default message handler.

    Resolves output ports according the port map and forwards incoming messages
    to the scheduler.
    """

    def __init__(self, scheduler, portmap):
        self.scheduler = scheduler
        self.portmap = portmap

    def dispatch(self, port, item):
        self.scheduler.send(item, self.portmap[port])


class SchedulerProtocol(protocol.Protocol):
    """A client protocol suitable to control a remote scheduler.

    Attributes:
        builder: A message builder for outgoing messages.
        handler: A message handler for incoming messages.
        handler: A message parser for incoming messages.
    """

    # pylint: disable=invalid-name

    builder = None
    handler = None
    parser = None
    _stopped = None

    def loseConnection(self):
        """
        Stop the remote scheduler and disconnect the transport.

        Returns
            defer.Deferred: Fires when the connection has been closed.
        """
        if not self._stopped:
            self._stopped = defer.Deferred()
            self.transport.loseConnection()

        return self._stopped

    def sendMessage(self, port, item):
        """
        Send an outgoing message to the specified input port on the remote
        scheduler.
        """
        assert self.builder is not None, \
            'Protocol factory must set a builder for outgoing messages'

        msg = {'port': port, 'item': item}
        self.transport.write(self.builder.message(msg))

    def dataReceived(self, data):
        assert self.parser is not None, \
            'Protocol factory must set a parser for incoming messages'
        assert self.parser is not None, \
            'Protocol factory must set a handler for incoming messages'

        self.parser.push(data)
        for msg in self.parser.messages():
            self.handler.dispatch(msg['port'], msg['item'])

    def connectionLost(self, reason=protocol.connectionDone):
        if self._stopped:
            self._stopped.callback(self)
            self._stopped = None
        else:
            reason.raiseException()

@implementer(interfaces.IHalfCloseableProtocol)
class SchedulerServerProtocol(SchedulerProtocol):
    scheduler = None

    def readConnectionLost(self):
        self.scheduler.stop(self)

    def writeConnectionLost(self):
        pass

class SchedulerClientFactory(protocol.ClientFactory):
    """
    Client protocol factory for remote schedulers.
    """

    def __init__(self, builder_factory=None, handler=None, parser_factory=None):
        self.builder_factory = builder_factory
        self.handler = handler
        self.parser_factory = parser_factory

    def buildProtocol(self, addr):
        proto = self.protocol()
        proto.builder = self.builder_factory and self.builder_factory()
        proto.handler = self.handler
        proto.parser = self.parser_factory and self.parser_factory()
        return proto

class SchedulerServerFactory(protocol.ServerFactory):
    """
    Server protocol factory for remote schedulers.
    """

    def __init__(self, scheduler, builder_factory=None, handler=None, parser_factory=None):
        self.connected = defer.Deferred()
        self.scheduler = scheduler
        self.builder_factory = builder_factory
        self.handler = handler
        self.parser_factory = parser_factory

    def buildProtocol(self, addr):
        if not self.connected.called:
            proto = self.protocol()
            proto.scheduler = self.scheduler
            proto.builder = self.builder_factory and self.builder_factory()
            proto.handler = self.handler
            proto.parser = self.parser_factory and self.parser_factory()
            self.connected.callback(proto)
            return proto

class ClientEndpointMixin(object):
    """
    A mixin providing methods to components relying on twisted client endpoints.

    Attributes:
        strport (string): The strport description specifying the server to
            connect to or the process to start.
        peer (IProtocol): The protocol instance when connected to the endpoint
            (managed by this mixin). Use it from a subclass to send messages to
            the peer.
    """

    peer = None
    strport = None

    def get_client_protocol_factory(self, scheduler, reactor):
        """
        Returns a client protocol factory instance.

        Must be overridden by a subclass.
        """
        raise NotImplementedError('Subclass must implement this method')

    @defer.inlineCallbacks
    def attach(self, scheduler, reactor):
        factory = self.get_client_protocol_factory(scheduler, reactor)
        endpoint = clientFromString(reactor, self.strport)
        self.peer = yield endpoint.connect(factory)

    @defer.inlineCallbacks
    def detach(self):
        if self.peer:
            yield self.peer.loseConnection()
        self.peer = None

class ServerEndpointMixin(object):
    """
    A mixin providing methods to components relying on twisted server endpoints.

    Attributes:
        strport (string): The strport description specifying the server to
            connect to or the process to start.
        peer (IProtocol): The protocol instance when connected to the endpoint
            (managed by this mixin). Use it from a subclass to send messages to
            the peer.
    """

    peer = None
    strport = None

    def get_server_protocol_factory(self, scheduler, reactor):
        """
        Returns a server protocol factory instance.

        Must be overridden by a subclass.
        """
        raise NotImplementedError('Subclass must implement this method')

    @defer.inlineCallbacks
    def attach(self, scheduler, reactor):
        factory = self.get_server_protocol_factory(scheduler, reactor)
        endpoint = serverFromString(reactor, self.strport)
        endpoint.listen(factory)
        self.peer = yield factory.connected

    @defer.inlineCallbacks
    def detach(self):
        if self.peer:
            yield self.peer.loseConnection()
        self.peer = None

class StrportGeneratorMixin(object):
    """
    A mixin which simplifies generating strport strings.
    """

    STRPORT_ESCAPE = r'([:=\\])'

    def strport_generate(self, name, *args, **kwds):
        """
        Returns a strport string following the method signature.
        """
        result = self._strport_escape(name)

        for arg in args:
            if arg is not None:
                result = '{:s}:{:s}'.format(result, self._strport_escape(arg))

        for key, value in kwds.items():
            if value is not None:
                result = '{:s}:{:s}={:s}'.format(result, key,
                                                 self._strport_escape(value))

        return result

    def _strport_escape(self, value):
        return re.sub(self.STRPORT_ESCAPE, r'\\\1', str(value))
