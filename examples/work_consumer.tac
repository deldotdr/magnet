import sys
import time

from twisted.application import internet
from twisted.application import service
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.protocols import basic
from twisted.internet import task
from twisted.python import log

from magnet.protocol import ClientCreator
from magnet.protocol import LogProtocol



class WorkProtocol(basic.LineReceiver):

    def lineReceived(self, line):
        try:
            self.do_work(line)
        except:
            log.err('Work error')
            return
        self.transport.ack()

    def do_work(self, work):
        log.msg("Doing work... ", int(work))
        time.sleep(int(work))
        return

class WorkFactory(protocol.ClientFactory):
    protocol = WorkProtocol


@inlineCallbacks
def main(application):
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    f = WorkFactory()

    preactor.connectWorkConsumer('work', f)

    log_context = "work_consumer"
    LogProtocol.log_context = log_context

    log_client_creator = ClientCreator(reactor, preactor, LogProtocol)
    log_client = yield log_client_creator.connectWorkProducer('log')

    # application.setComponent(log.ILogObserver, log_client.sendLog)
    log.addObserver(log_client.sendLog)

    preactor.run()

application = service.Application('worker')
main(application)
