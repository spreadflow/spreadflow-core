from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os
import tempfile

from twisted.application import service
from twisted.internet import error, task
from twisted.logger import globalLogPublisher, ILogObserver
from twisted.python import usage
from zope.interface import provider

from spreadflow_core.config import config_eval
from spreadflow_core.eventdispatcher import EventDispatcher
from spreadflow_core.scheduler import Scheduler, JobEvent
from spreadflow_core.dsl import \
    AliasResolverPass, \
    ComponentsPurgePass, \
    EventHandlersPass, \
    PartitionBoundsPass, \
    PartitionControllersPass, \
    PartitionExpanderPass, \
    PartitionWorkerPass, \
    PortsValidatorPass, \
    portmap
from spreadflow_core.dsl.compiler import \
    AddTokenOp, \
    stream_extract, \
    token_map
from spreadflow_core.dsl.tokens import \
    ConnectionToken, \
    EventHandlerToken, \
    PartitionSelectToken

class Options(usage.Options):
    optFlags = [
        ['oneshot', 'o', "Exit after initial execution of the network"],
        ['multiprocess', 'p', "Launch a separate process for each chain"],
    ]

    optParameters = [
        ['confpath', 'c', None, 'Path to configuration file'],
        ['queuestatus', None, None, 'Path where status should be written to'],
        ['partition', None, None, 'Run the given partition of the graph (internal)'],
        ['protocol', None, None, 'The IPC protocol to use when running a partition (internal)'],
    ]


def makeService(options):
    return SpreadFlowService(options)


class SpreadFlowService(service.Service):
    def __init__(self, options):
        self.options = options
        self._scheduler = None
        self._eventdispatcher = None

    def startService(self):
        super(SpreadFlowService, self).startService()

        if self.options['confpath']:
            confpath = self.options['confpath']
        else:
            confpath = os.path.join(os.getcwd(), 'spreadflow.conf')

        stream = config_eval(confpath)

        pipeline = list()
        pipeline.append(AliasResolverPass())
        pipeline.append(PortsValidatorPass())

        if self.options['multiprocess']:
            pipeline.append(PartitionExpanderPass())
            pipeline.append(PartitionBoundsPass())
            if self.options['partition']:
                pipeline.append(PartitionWorkerPass())
                partition = self.options['partition']
                stream.append(AddTokenOp(PartitionSelectToken(partition)))
            else:
                pipeline.append(PartitionControllersPass())

        pipeline.append(ComponentsPurgePass())
        pipeline.append(EventHandlersPass())

        for compiler_step in pipeline:
            stream = compiler_step(stream)

        self._eventdispatcher = EventDispatcher()

        if self.options['oneshot']:
            self._eventdispatcher.add_listener(JobEvent, 0, self._oneshot_job_event_handler)

        connection_ops, stream = stream_extract(stream, ConnectionToken)
        self._scheduler = Scheduler(portmap(connection_ops), self._eventdispatcher)

        event_handler_ops, stream = stream_extract(stream, EventHandlerToken)
        event_handlers = token_map(event_handler_ops).values()
        for event_type, priority, callback in event_handlers:
            self._eventdispatcher.add_listener(event_type, priority, callback)

        if self.options['queuestatus']:
            statuslog = SpreadFlowQueuestatusLogger(self.options['queuestatus'])
            statuslog.watch(1, self._scheduler)
            globalLogPublisher.addObserver(statuslog.logstatus)

        self._scheduler.run().addBoth(self._stop)

    def stopService(self):
        super(SpreadFlowService, self).stopService()
        return self._scheduler.join()

    def _stop(self, result):
        from twisted.internet import reactor
        try:
            reactor.stop()
        except error.ReactorNotRunning:
            pass
        return result

    def _oneshot_job_event_handler(self, event):
        scheduler = event.scheduler
        completed = event.completed

        def _stop_scheduler_when_done(result):
            if len(scheduler.pending) == 0:
                scheduler.stop(self)
            return result

        completed.addCallback(_stop_scheduler_when_done)

class SpreadFlowQueuestatusLogger(object):
    def __init__(self, path):
        self.path = path
        self.status = None
        self.task = None

    @provider(ILogObserver)
    def logstatus(self, event):
        if "log_failure" in event:
            self._dumpstatus("failed")

    def watch(self, interval, scheduler):
        call = task.LoopingCall(self._dumppending, scheduler)
        call.start(interval)

    def _dumppending(self, scheduler):
        self._dumpstatus(str(len(scheduler.pending)))

    def _dumpstatus(self, status):
        if self.status == "failed":
            return

        if status == None or status == self.status:
            return

        self.status = status

        dirname, basename = os.path.split(self.path)
        temp = tempfile.NamedTemporaryFile(prefix=basename, dir=dirname, delete=False)
        temp.write(status)
        temp.close()
        os.rename(temp.name, self.path)
