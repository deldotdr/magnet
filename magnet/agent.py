

import urllib2
from magnet import amqp

class Agent(amqp.AMQPConnectionService):
    """The most basic agent needs only to supply an AMQP channel for it's
    actors to create channels.

    The ideal use of this is to pass simple handling functions for actors
    to use when they get a message.
    """
    pass


class EC2ManagementAgent(Agent):
    """Rough sculpture prototype of an Agent used to contextualize an
    Amazon EC2 instance.
    """

    instance_id = None

    def get_user_and_meta_data(self):
        user_data = urllib2.urlopen(INSTANCE_DATA_BASE_URL + "user-data").read()
        user_data = dict([d.split('=') for d in user_data.split()])
        self.user_data = user_data
        public_dns_name = urllib2.urlopen(INSTANCE_DATA_BASE_URL + "meta-data/public-hostname").read()
        private_dns_name = urllib2.urlopen(INSTANCE_DATA_BASE_URL + "meta-data/local-hostname").read()
        instance_id = urllib2.urlopen(INSTANCE_DATA_BASE_URL + "meta-data/instance-id").read()
        ami_launch_index = str(urllib2.urlopen(INSTANCE_DATA_BASE_URL + 'meta-data/ami-launch-index').read())
        self.instance_id = instance_id
        meta_data = {
                'this_public_dns_name':public_dns_name,
                'this_private_dns_name':private_dns_name,
                'this_instance_id':instance_id,
                'instance_id':instance_id,
                'ami_launch_index':ami_launch_index,
                }
        self.meta_data = meta_data
        self.user_meta_data = {}
        self.user_meta_data.update(user_data)
        self.user_meta_data.update(meta_data)

class IAgent

class ControlAgent(Agent):
    """Prelim Agent class fo contextualizing and controling an application
    node within an EPU

    """

    def action_

