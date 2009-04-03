"""
 - Sub-class magnet.pole.BasePole
 - Define methods that begin with 'action_'. Don't define an __init__, as
   it is already defined in BasePole
   
   
"""



from twisted.internet.utils import getProcessOutput

from magnet import pole


class Say(pole.BasePole):

    def action_say(self, message_object):
        to_say = message_object['payload']
        d = getProcessOutput("/usr/bin/say", args=(to_say,))
        return None

# The plugin system uses this
say = Say(routing_pattern='test')
