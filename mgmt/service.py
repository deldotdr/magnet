
from twisted.internet import defer
from twisted.internet import reactor
from twisted.application import service
from twisted.application import internet

from magnet.agent.service import AMQPService
from magnet.agent.service import TopicConsumer
from magnet.agent.service import TopicCommandProducer





class Provisioner(service.MultiService):
    """The Service to start all services.
    """
    pass




default_AMI_config = {
        'node_type':None,
        'ami_id':None,
        'num_insts':None
        'host':None,
        'port':5672,
        'vhost':'/',
        'username':'guest',
        'password':'guest',
        }
        

class Unit(AMQPService):

    reservation = None

    def __init__(self, config, ec2):
        config = default_AMI_config.update(config)
        self.config = config
        AMQPService.__init__(self, config)
        self.ec2 = ec2

    def startService(self):
        ami_id = self.config['ami_id']
        N = self.config['num_insts']
        node_type = self.config['node_type']
        print 'Starting ', N, 'nodes of ', node_type, ami_id
        self.reservation = self.ec2.run_instances(ami_id, min_count=N, max_count=N)
        TopicConsumer({'node_type':node_type}).setServiceParent(self)
        TopicCommandProducer({'node_type':node_type}).setServiceParent(self)
        AMQPService.startService(self)

    def stopService(self):
        print 'Stopping ', self.config['node_type'], ' Nodes'
        self.reservation.stop_all()





