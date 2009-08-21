import sys
import random

from twisted.application import service
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.internet import task
from twisted.protocols import basic
from twisted.python import log


from magnet.protocol import ClientCreator
from magnet.protocol import LogProtocol


class WorkClient(basic.LineReceiver):
    def __init__(self):
        self.count = 0

    def do_work(self, t):
        to_send = str(t)
        log.msg('Produced work %s' % to_send)
        self.sendLine(to_send)

    def lineReceived(self, line):
        log.msg('Received: %s', line)


def sleep_time(client):
    t = random.randint(1, 5)
    client.do_work(t)


@inlineCallbacks
def main(application):
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    # ClientCreator for connectMS
    client_creator = ClientCreator(reactor, preactor, WorkClient)
    d = client_creator.connectWorkProducer('work')
    work_client = yield d
    
    l = task.LoopingCall(sleep_time, work_client)
    l.start(1)

    log_context = "work_producer"
    LogProtocol.log_context = log_context

    log_client_creator = ClientCreator(reactor, preactor, LogProtocol)
    log_client = yield log_client_creator.connectSimpleProducer('log')
    log_client.sendLog('testing')

    # application.setComponent(log.ILogObserver, log_client.sendLog)
    log.addObserver(log_client.sendLog)




application = service.Application('workproducer')
main(application)
