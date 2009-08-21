import sys

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.python import log
from twisted.protocols import basic


from magnet.protocol import ClientCreator
from magnet.protocol import RequestResponseLineReceiver


log.startLogging(sys.stdout)

class AddClient(RequestResponseLineReceiver):

    def add(self, a, b):
        to_send = 'add, %d, %d' % (a, b)
        return self.makeRequest(to_send)




@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    # ClientCreator for connectMS
    client_creator = ClientCreator(reactor, preactor, AddClient)
    d = client_creator.connectMS('add-service')
    add_client = yield d
    
    # use client
    ans = yield add_client.add(2, 2)
    log.msg('resopnse: %s' % ans)

    ans = yield add_client.add(5, 2)
    log.msg('resopnse: %s' % ans)



if __name__ == '__main__':
    main()
    reactor.run()

