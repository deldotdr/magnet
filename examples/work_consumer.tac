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


# log.startLogging(sys.stdout)

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
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    f = FactorFactory()

    preactor.connectWorkConsumer('work', f)
    preactor.run()

application = service.Application('worker')
main()
