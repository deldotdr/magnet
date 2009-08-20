import sys
import time

from twisted.application import internet
from twisted.application import service
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.protocols import basic
from twisted.internet import task
from twisted.python import log

from magnet.protocol import ClientCreator
from magnet.protocol import LogProtocol




@inlineCallbacks
def main(application):
    from magnet.preactor import Preactor
    preactor = yield Preactor()


    log_client_creator = ClientCreator(reactor, preactor, LogProtocol)
    log_client = yield log_client_creator.connectSimpleConsumer('log')

    preactor.run()

application = service.Application('worker')
main(application)
