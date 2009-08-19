import sys

from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet.defer import inlineCallbacks
from twisted.python import log

log.startLogging(sys.stdout)



class EchoMessage(protocol.Protocol):

    def dataReceived(self, data):
        print 'data from client:', data
        self.transport.write(data)
        # self.transport.loseConnection()

class EchoFactory(protocol.ServerFactory):
    protocol = EchoMessage


@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    f = EchoFactory()

    preactor.listenMS('echo-server', f)
    preactor.run()


if __name__ == '__main__':
    main()
    reactor.run()
