# -*- coding: utf-8 -*-

"""
Client endpoint string parser plugin for spreadflow worker process.
"""

# pylint: disable=invalid-name,too-few-public-methods

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os
import sys

from twisted.internet.endpoints import ProcessEndpoint
from twisted.internet.interfaces import IStreamClientEndpointStringParserWithReactor
from twisted.plugin import IPlugin
from twisted.python.runtime import platform
from zope.interface import implementer


@implementer(IPlugin, IStreamClientEndpointStringParserWithReactor)
class SpreadflowWorkerProcessEndpoint(object):
    """
    Client endpoint string parser plugin for spreadflow worker process.

    @ivar prefix: See
        L{IStreamClientEndpointStringParserWithReactor.prefix}.
    """
    prefix = 'spreadflow-worker'

    @staticmethod
    def _parseWorker(reactor, name, executable=None):
        """
        Constructs a process endpoint for a spreadflow worker process.

        Args:
            reactor: The reactor passed to ``clientFromString``.
            name (str): The partition name this worker should operate on.
            executable (Optional[str]). Path to the spreadflow-twistd command.

        Returns:
            A client process endpoint
        """
        if executable is None:
            executable = sys.argv[0]

        logger = 'spreadflow_core.scripts.spreadflow_twistd.StderrLogger'
        args = [executable, '-n', '--logger', logger, '--multiprocess',
                '--partition', name]

        if not platform.isWindows():
            args.extend(['--pidfile', ''])

        fds = {0: "w", 1: "r", 2: 2}

        return ProcessEndpoint(reactor, executable, args=args,
                               env=os.environ.copy(), childFDs=fds)

    def parseStreamClient(self, reactor, *args, **kwargs):
        """
        Redirects to another function ``_parseWorker`` tricks zope.interface
        into believing the interface is correctly implemented, since the
        signature is (``reactor``, ``*args``, ``**kwargs``.  See
        ``_parseWorker`` for an the specific signature description for this
        endpoint parser.

        Args:
            reactor: The reactor passed to ``clientFromString``.
            *args: The positional arguments in the endpoint description.
            **kwargs: The named arguments in the endpoint description.

        Returns:
            A client process endpoint
        """
        return self._parseWorker(reactor, *args, **kwargs)

processEndpoint = SpreadflowWorkerProcessEndpoint()
