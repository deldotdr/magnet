
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks





@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    pkt = preactor.pocket()
    pkt.delete_name('dxCacheController')
    # pkt.purge_name('dxCacheController')
    # pkt.purge_name('log')



if __name__ == '__main__':
    main()
    reactor.run()
