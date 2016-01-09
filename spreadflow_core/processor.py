from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from twisted.logger import Logger, LogLevel


class SyntheticSource(object):
    def __init__(self, items):
        self.items = items

    def attach(self, scheduler, reactor):
        for delay, item in self.items:
            reactor.callLater(delay, scheduler.send, item, self)

    def __call__(self, item, send):
        send(item, self)


class DebugLog(object):
    """
    A minimal processor which simply logs every item received.
    """

    log = Logger()

    def __init__(self, message='Item received: {item}', level='debug'):
        self.level = LogLevel.levelWithName(level)
        self.message = message

    def __call__(self, item, send):
        self.log.emit(self.level, self.message, item=item)
        send(item, self)
