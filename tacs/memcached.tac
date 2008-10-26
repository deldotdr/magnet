"""
Generic agent configuration.
The type of node is realized using the aws Instance data url.

The user-data should be the node-type.

Preliminary version, 10-17-2008
 - Dorian Raymer <deldotdr@gmail.com>

"""

import urllib2

from twisted.application import service

from magnet.agent.service import Agent
from magnet.agent.service import ReportHostname
from magnet.agent.service import RunScript
from magnet.agent.service import Status
from magnet.agent.service import AllNodeDnsConsumer
from magnet.agent.service import ConfigTemplateConsumer
from magnet.agent.service import INSTANCE_DATA_BASE_URL

import magnet
spec_path = magnet.__path__[0] + '/amqp0-8.xml'

node_type = 'bad-user-data'
user_data = urllib2.urlopen(INSTANCE_DATA_BASE_URL + 'user-data').read()
user_data = dict([d.split('=') for d in user_data.split()])
node_type = user_data['node_type']

print 'Node_type (from user data): ', node_type

# Client config object
config = {
        'host':'rabbitmq.amoeba.ucsd.edu',
        'port':5672,
        'vhost':'/',
        'spec':spec_path,
        'username':'guest',
        'password':'guest',
    }


agent_service = Agent(config)

config_task_announce = {
            'exchange':'announce',
            'routing_key':node_type,
            'node_type':node_type,
            }

config_task_status = {
            'exchange':'status',
            'routing_key':node_type,
            'node_type':node_type,
            }

config_task_runscript = {
            'exchange':'command',
            'routing_key':node_type,
            'node_type':node_type,
            }

config_task_consume_config_templ = {
            'exchange':'config_templ',
            'routing_key':node_type,
            'node_type':node_type,
            }

config_task_dns = {
            'exchange':'dns',
            'routing_key':node_type,
            'node_type':node_type,
            }




task_announce = ReportHostname(config_task_announce)
task_status =  Status(config_task_status)
task_dns = AllNodeDnsConsumer(config_task_dns)
task_runscript = RunScript(config_task_runscript)
task_config_templ = ConfigTemplateConsumer(config_task_consume_config_templ)

task_announce.setServiceParent(agent_service)
task_status.setServiceParent(agent_service)
task_dns.setServiceParent(agent_service)
task_runscript.setServiceParent(agent_service)
task_config_dict.setServiceParent(agent_service)


application = service.Application('ContextAgent')

agent_service.setServiceParent(service.IServiceCollection(application))
