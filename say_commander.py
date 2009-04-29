#!/usr/bin/env python


from magnet import pole
from magnet import field
from twisted.internet import reactor
import logging

class Talker(pole.SendOne):
    def action_reply(self, message_object):
        logging.info('Got reply message: %s' % message_object['payload'])
        return None

    def doSend(self, msgString):
        logging.basicConfig(level=logging.DEBUG, \
                        format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')
        logging.info('Sending say message')
        self.send_when_running('test', 'say', msgString)

# Instantiate object, invoke
talker = Talker()
talker.doSend('hello, world!')
c = field.IAMQPClient(talker)
connector = field.AMQPClientConnectorService(reactor, c)
connector.connect(host='amoeba.ucsd.edu', spec_path='magnet/spec/amqp0-8.xml')
connector.startService()
reactor.run()
