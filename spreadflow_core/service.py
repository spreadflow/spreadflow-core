from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os

from twisted.application import service
from twisted.internet import defer
from twisted.python import usage

from spreadflow_core.config import config_eval
from spreadflow_core.decorator import OneshotDecoratorGenerator
from spreadflow_core.decorator import ThreadpoolDecoratorGenerator
from spreadflow_core.scheduler import Scheduler

class Options(usage.Options):
    optFlags = [
        ['oneshot', 'o', "Exit after initial execution of the network"],
        ['threaded', 't', "Defer blocking processes to a threadpool"],
    ]

    optParameters = [
        ['confpath', 'c', None, 'Path to configuration file']
    ]


def makeService(options):
    return SpreadFlowService(options)


class SpreadFlowService(service.Service):
    def __init__(self, options):
        self.options = options
        self._scheduler = None

    def startService(self):
        super(SpreadFlowService, self).startService()

        if self.options['confpath']:
            confpath = self.options['confpath']
        else:
            confpath = os.path.join(os.getcwd(), 'spreadflow.conf')

        flowmap = config_eval(confpath)

        if self.options['threaded']:
            flowmap.decorators.append(ThreadpoolDecoratorGenerator())

        if self.options['oneshot']:
            oneshot = OneshotDecoratorGenerator()
            flowmap.decorators.insert(0, oneshot)

        self._scheduler = Scheduler(flowmap)
        self._scheduler.done.addBoth(self._stop)
        return self._scheduler.start()

    def stopService(self):
        super(SpreadFlowService, self).stopService()
        return self._scheduler.join()

    def _stop(self, result):
        from twisted.internet import reactor
        reactor.stop()
