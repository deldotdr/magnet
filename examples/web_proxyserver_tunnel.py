import sys

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web import server, proxy
from twisted.web.client import HTTPClientFactory

log.startLogging(sys.stdout)

@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    f = server.Site(proxy.ReverseProxyResource('amoeba.ucsd.edu', 80, ''))

    preactor.listenMS('test-http-server', f)

if __name__ == '__main__':
    main()
    reactor.run()
