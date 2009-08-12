
from twisted.internet.defer import inlineCallbacks

from misted.amqp import AMQPClientCreator

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672


@inlineCallbacks
def make_p_reactor(reactor):
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)

    p_reactor = PocketReactor(reactor, client)
    return p_reactor