import sys

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.python import log

log.startLogging(sys.stdout)

from twisted.web.client import HTTPClientFactory




def show_page(page):
    print 'Page received'
    print page[0:100]
    print '...'
    print page[-100:]
    f = open('test.html', 'w')
    f.write(page)
    f.close()


@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    f = HTTPClientFactory('http://amoeba.ucsd.edu')
    f.deferred.addCallback(show_page)

    preactor.connectMS('test-http-server', f)
    preactor.run()



if __name__ == '__main__':
    main()
    reactor.run()
