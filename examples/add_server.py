import sys

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.protocols import basic
from twisted.internet import task

from twisted.web.client import HTTPClientFactory
from twisted.web import server, proxy, resource, static
from twisted.python import log


log.startLogging(sys.stdout)

class AddProtocol(basic.LineReceiver):

    def lineReceived(self, line):
        print self, self.transport, 'Add request: ', line
        if line[0:3] == 'add':
            _, a, b = line.split(',')
            c = self.add(int(a), int(b))
            self.sendLine(str(c))

    def add(self, a, b):
        """Simple example of specific protocol functionality
        """
        return a + b

class AddFactory(protocol.ServerFactory):
    protocol = AddProtocol



@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    f = AddFactory()

    preactor.listenMS('add-service', f)
    preactor.run()

if __name__ == '__main__':
    main()
    reactor.run()
