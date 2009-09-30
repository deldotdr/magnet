import sys

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.protocols import basic


from magnet.protocol import ClientCreator


log.startLogging(sys.stdout)

class AddClient(basic.LineReceiver):

    def add(self, a, b):
        to_send = 'add, %d, %d' % (a, b)
        self.sendLine(to_send)

    def lineReceived(self, line):
        log.msg('Result: '+ line)



@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    # ClientCreator for connectMS
    client_creator = ClientCreator(reactor, preactor, AddClient)
    d = client_creator.connectMS('add-service')
    add_client = yield d
    
    # use client
    add_client.add(2, 2)
    add_client.add(23, 2)
    add_client.transport.loseConnection()
    add_client.add(1000, 99999)



if __name__ == '__main__':
    main()
    reactor.run()

