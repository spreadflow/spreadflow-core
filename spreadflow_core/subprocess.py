# -*- coding: utf-8 -*-

"""
Components for multiprocessing.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from spreadflow_core.remote import ClientEndpointMixin, MessageHandler, SchedulerClientFactory, SchedulerProtocol, SchedulerServerFactory, SchedulerServerProtocol, ServerEndpointMixin
from spreadflow_core.format import PickleMessageParser, PickleMessageBuilder

class SubprocessWorker(ServerEndpointMixin):
    """
    Subprocess worker component.

    Arguments:
        ins (string[]): A list of names for the input ports.
        outs (string[]): A list of names for the output ports.
    """

    def __init__(self, innames=None, outnames=None):
        self.strport = 'stdio:'
        self._innames = innames or []
        self._outnames = outnames or []
        self._ins = {}
        self._outs = {}

        for name in innames:
            self._ins[name] = lambda item, send, port=name: self.peer.sendMessage(port, item)
        for name in outnames:
            self._outs[name] = object()

    @property
    def ins(self):
        return [self._ins[name] for name in self._innames]

    @property
    def outs(self):
        return [self._outs[name] for name in self._outnames]

    def get_server_protocol_factory(self, scheduler, reactor):
        handler = MessageHandler(scheduler, self._outs)
        return SchedulerServerFactory.forProtocol(SchedulerServerProtocol,
                                                  scheduler,
                                                  builder_factory=PickleMessageBuilder,
                                                  handler=handler,
                                                  parser_factory=lambda: PickleMessageParser(2**23))

class SubprocessController(ClientEndpointMixin):
    """
    Subprocess controller component.

    Arguments:
        ins (string[]): A list of names for the input ports.
        outs (string[]): A list of names for the output ports.
    """

    def __init__(self, name, innames=None, outnames=None):
        self.strport = 'spreadflow-worker:{:s}'.format(name)
        self._innames = innames or []
        self._outnames = outnames or []
        self._ins = {}
        self._outs = {}

        for name in innames:
            self._ins[name] = lambda item, send, port=name: self.peer.sendMessage(port, item)
        for name in outnames:
            self._outs[name] = object()

    @property
    def ins(self):
        return [self._ins[name] for name in self._innames]

    @property
    def outs(self):
        return [self._outs[name] for name in self._outnames]

    def get_client_protocol_factory(self, scheduler, reactor):
        handler = MessageHandler(scheduler, self._outs)
        return SchedulerClientFactory.forProtocol(SchedulerProtocol,
                                                  builder_factory=PickleMessageBuilder,
                                                  handler=handler,
                                                  parser_factory=lambda: PickleMessageParser(2**23))
