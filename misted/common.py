"""
NOT USED
Part of initial test case
"""
from twisted.internet.defer import inlineCallbacks, returnValue


RABBIT_MQ_HOST = "amoeba.ucsd.edu"
RABBIT_MQ_PORT = 5672

VHOST = "/"
EXCHANGE_NAME = "test_mshttp"
QUEUE_NAME = "test_mshttp_client"
ROUTING_KEY = "test_mshttp_server"
CONSUMER_TAG = "test_mshttp_client"

NON_PERSISTENT = 1
PERSISTENT = 2

credentials = {"LOGIN": "guest", "PASSWORD": "guest"}


@inlineCallbacks
def getConnection(client):
    conn = yield client.connectTCP(
        RABBIT_MQ_HOST, RABBIT_MQ_PORT)
    # start the connection negotiation process, sending security mechanisms
    # which the client can use for authentication
    yield conn.start(credentials)
    returnValue(conn)


@inlineCallbacks
def getChannel(conn):
    # create a new channel that we'll use for sending messages; we can use any
    # numeric id here, only one channel will be created; we'll use this channel
    # for all the messages that we send
    chan = yield conn.channel(3)
    # open a virtual connection; channels are used so that heavy-weight TCP/IP
    # connections can be used my multiple light-weight connections (channels)
    yield chan.channel_open()
    returnValue(chan)

