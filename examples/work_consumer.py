import sys
import time

from sympy import factorint

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.protocols import basic
from twisted.internet import task

from twisted.python import log


log.startLogging(sys.stdout)

class Factor(basic.LineReceiver):

    def lineReceived(self, line):
        try:
            # self.factor(line)
            self.sleep(line)
        except:
            log.err('Factor error')
            return
        self.transport.ack()

    def factor(self, n):
        """Simple example of specific protocol functionality
        """
        log.msg('Factor ', n)
        f = factorint(long(n))
        log.msg('Factors: ', str(f))
        return 

    def sleep(self, t):
        log.msg("Sleep ", int(t))
        time.sleep(int(t))
        return

class FactorFactory(protocol.ClientFactory):
    protocol = Factor

@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    f = FactorFactory()

    preactor.connectWorkConsumer('work', f)
    preactor.run()

if __name__ == '__main__':
    main()
    reactor.run()
