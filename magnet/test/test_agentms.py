
import os
import sys

from twisted.python import log
from twisted.internet import reactor
from twisted.internet.utils import getProcessOutput

from magnet import pole
from magnet import field

import magnet
spec_path_def = os.path.join(magnet.__path__[0], 'spec', 'amqp0-8.xml')

log.startLogging(sys.stdout)
log.msg('Test agent ms')

class Say(pole.Role):
    """
    """
    def sendOK(self, result):
        reply = {'method': 'reply', 'payload': 'ok'}
        self.sendMessage(reply, 'test')

    def sendError(self, failure):
        reply = {'method': 'reply', 'payload': 'got an error running say'}
        self.sendMessage(reply, 'test')

    def action_say(self, message_object):
        to_say = message_object['payload']
        log.msg('Got say message: %s' % message_object['payload'])
        d = getProcessOutput('/usr/bin/say', ['-v', 'Fred', to_say])
        d.addCallback(self.sendOK).addErrback(self.sendError)
        return None


say_role = Say('Control')


EXCHANGE = 'mac-demo'
EXCHANGE = 'magnet'
RESOURCE = 'apple'

macAgent = pole.Agent(EXCHANGE, RESOURCE)
macAgent.addRole(say_role)

log.msg('make Channel manager')
manlay = field.ChannelManagementLayer()
manlay.addAgent(macAgent)

log.msg('make amqp connector')
amqpConnector = field.AMQPConnector(manlay, host='amoeba.ucsd.edu',
        spec_path=spec_path_def)

log.msg('connect')
amqpConnector.connect()
reactor.run()


