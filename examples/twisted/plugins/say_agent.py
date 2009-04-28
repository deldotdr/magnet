"""
 - Sub-class magnet.pole.BasePole
 - Define methods that begin with 'action_'. Don't define an __init__, as
   it is already defined in BasePole


"""



from twisted.internet.utils import getProcessOutput

from magnet import pole
import logging

class Say(pole.BasePole):

    def sendOK(self, result):
        reply = {'method': 'reply', 'payload': 'ok'}
        self.sendMessage(reply, 'test')

    def sendError(self, failure):
        reply = {'method': 'reply', 'payload': 'got an error running say'}
        self.sendMessage(reply, 'test')

    def action_say(self, message_object):
        to_say = message_object['payload']
        logging.debug('Got say message: %s' % message_object['payload'])
        d = getProcessOutput('/usr/bin/say', ['-v', 'pipe organ', to_say])
        d.addCallback(self.sendOK).addErrback(self.sendError)
        return None

logging.basicConfig(level=logging.DEBUG, \
                        format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')
# The plugin system uses this
say = Say(routing_pattern='test')
#
#class Talker(pole.SendOne):
#    pass

#talker = Talker()
#talker.send_when_running('test', 'say', 'oh say can you see')
