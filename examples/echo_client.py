import sys

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.python import log

log.startLogging(sys.stdout)



class MSClient(protocol.Protocol):

    def dataReceived(self, data):
        print 'echo from server:', data
        # self.transport.loseConnection()

    def connectionMade(self):
        self.transport.writeSequence('Hello, world!')

class MSClientFactory(protocol.ClientFactory):
    protocol = MSClient

@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    f = MSClientFactory()
    preactor.connectMS('echo-server', f)



if __name__ == '__main__':
    main()
    reactor.run()
