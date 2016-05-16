"""
Protocol and protocol factories allowing communication with remote instances.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from twisted.internet import defer, protocol
from twisted.internet.endpoints import clientFromString


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
        assert self.parser is not None, \
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
    _endpoint = None
    _factory = None

    def get_client_protocol_factory(self, scheduler, reactor):
        """
        Returns a client protocol factory instance.

        Must be overridden by a subclass.
        """
        raise NotImplementedError('Subclass must implement this method')

    def attach(self, scheduler, reactor):
        self._factory = self.get_client_protocol_factory(scheduler, reactor)
        self._endpoint = clientFromString(reactor, self.strport)

    @defer.inlineCallbacks
    def start(self):
        self.peer = yield self._endpoint.connect(self._factory)

    @defer.inlineCallbacks
    def join(self):
        if self.peer:
            yield self.peer.loseConnection()
        self.peer = None

    def detach(self):
        self._factory = None
        self._endpoint = None