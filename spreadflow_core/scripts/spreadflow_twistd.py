from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from twisted.application import service
from twisted.internet import error
from twisted.python import log, usage
from twisted.logger import textFileLogObserver
from twisted.scripts.twistd import ServerOptions as TwistdServerOptions
from twisted.scripts.twistd import _SomeApplicationRunner as TwistdApplicationRunner

import sys

from spreadflow_core import service as spreadflow_service

class SpreadFlowApplicationRunner(TwistdApplicationRunner):
    def createOrGetApplication(self):
        application = service.Application('spreadflow')
        srv = spreadflow_service.makeService(self.config)
        srv.setServiceParent(application)
        return application

class SpreadFlowServerOptions(TwistdServerOptions, spreadflow_service.Options):
    longdesc = ''

    def parseOptions(self, options=None):
        """Circumvent the parents implementation and forward directly to usage.Options"""
        usage.Options.parseOptions(self, options)

    def postOptions(self):
        """Ensure that no_save flag is always set"""
        TwistdServerOptions.postOptions(self)
        self['no_save'] = True

    def __getattribute__(self, attr):
        """Prevent access to subCommands"""
        if attr == 'subCommands':
            raise AttributeError

        return object.__getattribute__(self, attr)

def StderrLogger():
    return textFileLogObserver(sys.stderr)

class ExitOnFailure(object):
    def __init__(self, reactor=None):
        self.triggered = False
        self.reactor = reactor
        log.addObserver(self._failure_observer)

    def _failure_observer(self, log_entry):
        if 'failure' in log_entry:
            if self.reactor:
                reactor = self.reactor
            else:
                from twisted.internet import reactor

            reactor.callFromThread(self._exit, reactor)

    def _exit(self, reactor):
        self.triggered = True
        log.removeObserver(self._failure_observer)

        try:
            reactor.stop()
        except error.ReactorNotRunning:
            pass

def main():
    config = SpreadFlowServerOptions()
    try:
        config.parseOptions()
    except usage.error as ue:
        print(config)
        print("%s: %s" % (sys.argv[0], ue))
        sys.exit(2)

    ex = ExitOnFailure()
    SpreadFlowApplicationRunner(config).run()
    sys.exit(1 if ex.triggered else 0)
