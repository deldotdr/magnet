
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks





@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    pkt = preactor.pocket()
    pkt.delete_name('factor')

    preactor.run()



if __name__ == '__main__':
    main()
    reactor.run()
