import sys
import time

from sympy import factorint

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.protocols import basic
from twisted.internet import task

from twisted.python import log

from magnet.amqp import AMQPClientCreator
from magnet.core import PocketReactor

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_HOST = 'localhost'
BROKER_PORT = 5672

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
def main(reactor):
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)

    p_reactor = PocketReactor(reactor, client)

    f = FactorFactory()

    p_reactor.connectWorkConsumer('factor', f)
    p_reactor.run()

if __name__ == '__main__':
    main(reactor)
    reactor.run()
