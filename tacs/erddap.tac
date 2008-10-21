
import urllib2

from twisted.application import service

from magnet.agent.service import AMQPService
from magnet.agent.service import ReportHostname
from magnet.agent.service import RunScript
from magnet.agent.service import Status
from magnet.agent.service import ConfigDictConsumer
from magnet.agent.service import INSTANCE_DATA_BASE_URL

import magnet
spec_path = magnet.__path__[0] + '/amqp0-8.xml'

node_type = 'bad-user-data'
node_type = urllib2.urlopen(INSTANCE_DATA_BASE_URL + 'user-data').read()

# Client config object
config = {
        'host':'rabbitmq.amoeba.ucsd.edu',
        'port':5672,
        'vhost':'/',
        'spec':spec_path,
        'username':'guest',
        'password':'guest',
    }


agent_service = AMQPService(config)

config_task_report = {
            'exchange':'status',
            'routing_key':node_type,
            'node_type':node_type,
            'queue':node_type,
            }

config_task_status = {
            'exchange':'status',
            'routing_key':node_type,
            'node_type':node_type,
            'queue':node_type,
            }

config_task_runscript = {
            'exchange':'command',
            'routing_key':node_type,
            'node_type':node_type,
            'queue':node_type,
            }

config_task_consume_config_dict = {
            'exchange':'config_dict',
            'routing_key':node_type,
            'node_type':node_type,
            'queue':node_type,
            }



task_report = ReportHostname(config_task_report)
task_status =  Status(config_task_status)
task_runscript = RunScript(config_task_runscript)
task_config_dict = ConfigDictConsumer(config_task_consume_config_dict)

task_status.setServiceParent(agent_service)
task_report.setServiceParent(agent_service)
task_runscript.setServiceParent(agent_service)
task_config_dict.setServiceParent(agent_service)


application = service.Application('ContextAgent')

agent_service.setServiceParent(service.IServiceCollection(application))
