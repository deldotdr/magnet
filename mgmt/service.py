
from twisted.internet import defer
from twisted.internet import reactor
from twisted.application import service
from twisted.application import internet

from magnet.agent.service import AMQPService
from magnet.agent.service import Task
from magnet.agent.service import TopicConsumer
from magnet.agent.service import TopicCommandProducer
from magnet.agent.service import read_script_file


import magnet
magnet_path = magnet.__path__[0]
spec_path = magnet_path + '/amqp0-8.xml'




class Provisioner(service.MultiService):
    """The Service to start all services.
    This triggers commands to be sent to ami's

    """

    units_ready_for_load_app = 0
    units_ready_for_config_app = 0
    units_ready_for_run_app = 0
    units_finished = 0
    num_units = 0
    status = 0

    def startService(self):
        self.num_units = len([s for s in self])
        print 'Provisioner has ', self.num_units
        service.MultiService.startService(self)

    def setUnitReadyForLoadApp(self, unit_name):
        self.units_ready_for_load_app += 1
        self.sendDnsNames()
        if self.units_ready_for_load_app == self.num_units:
            self.status = 'load app'
            self.startLoadAppPhase()

    def setUnitReadyForConfigApp(self, unit_name):
        self.units_ready_for_config_app += 1
        if self.units_ready_for_config_app == self.num_units:
            self.status = 'config app'
            self.startConfigAppPhase()

    def setUnitReadyForRunApp(self, unit_name):
        self.units_ready_for_run_app += 1
        if  self.units_ready_for_run_app == self.num_units:
            self.status = 'run app'
            self.startRunAppPhase()

    def setUnitFinished(self, unit_name):
        """This marks the end of the Unit Starup phase.
        All nodes are running.
        Collect all hostnames.
        """
        self.units_finished += 1
        if self.units_finished == self.num_units:
            self.status = 'finished'

    def sendDnsNames(self):
        dns_names = {}
        for s in self:
            dns_names.update(s.get_private_dns_names_dict())
        self.dns_names = dns_names
        for s in self:
            s.sendDnsNames(dns_names)

    def startLoadAppPhase(self):
        for s in self.services:
            s.startLoadApp()

    def startConfigAppPhase(self):
        for s in self.services:
            s.startConfigApp()

    def startRunAppPhase(self):
        for s in self.services:
            s.startRunApp()

    def get_status_details(self):
        stats = []
        for s in self:
            stats.extend(s.get_status_details())
        print '================================='
        print 'Provision Status: ', self.status
        print 'Number of units:  ', self.num_units
        n = len(stats[0]) 
        for stat in stats:
            print '%s '*n % tuple(map(str,stat))
        print '================================='


class EC2Provisioner(Provisioner):

    ec2 = None

    def __init__(self, broker_host='localhost',
                        broker_port=5672,
                        broker_vhost='/',
                        broker_username='guest',
                        broker_password='guest',
                        amqp_spec_path=spec_path,
                        aws_access_key=None, 
                        aws_secret_access_key=None):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.broker_vhost = broker_vhost
        self.broker_username = broker_username
        self.broker_password = broker_password
        self.amqp_spec_path = spec_path
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key


    def startService(self):
        try:
            ec2 = boto.connect_ec2(self.aws_access_key, self.aws_secret_access_key)
        except:
            print 'boto connect ot ec2 error'

        self.ec2 = ec2
        Provisioner.startService(self)







default_AMI_config = {
        'node_type':None,
        'ami_id':None,
        'num_insts':None,
        'host':None,
        'port':5672,
        'vhost':'/',
        'username':'guest',
        'password':'guest',
        }
        

class InstanceAnnounceConsumer(TopicConsumer):

    name = 'inst_ann_consumer'
    exchange = 'announce'

    def operation(self, *args):
        instance_id = args[0]
        self.parent.setInstanceConfirmOn(instance_id)

class LoadAppResponseConsumer(TopicConsumer):

    name = 'load_app_resp_consumer'
    exchange = 'status'

    def operation(self, *args):
        instance_id = args[0]
        self.parent.setInstacnceConfirmLoaded(instance_id)

class ConfigAppResponseConsumer(TopicConsumer):

    name = 'config_app_resp_consumer'
    exchange = 'status'

    def operation(self, *args):
        instance_id = args[0]
        self.parent.setInstacnceConfirmConfiged(instance_id)

class RunAppResponseConsumer(TopicConsumer):

    name = 'run_app_resp_consumer'
    exchange = 'status'

    def operation(self, *args):
        instance_id = args[0]
        self.parent.setInstacnceConfirmRunning(instance_id)

class ConfigDictCommandProducer(Task):

    name = 'config_dict'
    type = 'produce'
    exchange = 'config_dict'

    def operation(self, *args):
        """
        Send completed config file 
        use dictionary of values to fill into template config file
        living on the provision exchange
        """
        msg = str(args[0])
        self.sendMessage(msg)

class SendConfigTemplate(Task):

    name = 'config_templ'
    exchange = 'config_templ'
    type = 'produce'

    def operation(self, config_dict):
        """config_dict is dict with key/values:
            config_templ:[str of config template]
            path:[path on ami it should be writen]
        """
        msg = str(config_dict)
        self.sendMessage(msg)

class SendAllDnsNames(Task):

    name = 'dns_names'
    exchange = 'dns_names'
    type = 'produce'

    def operation(self, dns_dict):
        """dns_dict:
            DNS_NODE_TYPE_N:[dns name]
        """
        msg = str(dns_dict)
        self.sendMessage(msg)



class Unit(AMQPService):

    reservation = None
    num_insts = 0
    instances_confirmed = 0
    status = 0
    apps_loaded = 0
    apps_configed = 0
    apps_running = 0
    ready_for_app_load = False
    ready_for_config = False
    public_dns_names = []

    def __init__(self, config):
        # config = default_AMI_config.update(config)
        self.config = config
        AMQPService.__init__(self, config)
        self.name = self.node_type = self.config['node_type']
        self.num_insts = self.config['num_insts']

    def get_public_dns_name(self):
        """return the dns name that makes most sense for this unit.
        """
        return self.public_dns_names[0]

    def get_private_dns_name(self):
        """return the dns name that makes most sense for this unit.
        """
        return self.private_dns_names[0]

    def get_all_private_dns_names(self):
        """return the dns names for all nodes in a unit
        """
        return self.private_dns_names

    def get_private_dns_names_dict(self):
        dns_dict = {}
        key_base = 'DNS_'+self.node_type.upper()+'_'
        for i in self.reservation.instances:
            k = key_base + str(i.ami_launch_index)
            v = str(i.private_dns_name)
            dns_dict[k] = v
        return dns_dict


    def startService(self):
        ami_id = self.config['ami_id']
        N = self.config['num_insts']
        node_type = self.config['node_type']
        user_data = 'node_type='+node_type
        self.status = 'starting'
        print 'Starting ', N, 'nodes of ', node_type, ami_id
        self.reservation = self.parent.ec2.run_instances(ami_id, min_count=N,
                max_count=N, user_data=user_data)
        InstanceAnnounceConsumer({'node_type':node_type, 'routing_key':node_type}).setServiceParent(self)
        TopicCommandProducer({'node_type':node_type, 'routing_key':node_type}).setServiceParent(self)
        SendConfigTemplate({'node_type':node_type, 'routing_key':node_type}).setServiceParent(self)
        SendAllDnsNames({'node_type':node_type, 'routing_key':node_type}).setServiceParent(self)
        AMQPService.startService(self)

    def stopService(self):
        if self.parent.status == 'finished':
            return
        print 'Stopping ', self.config['node_type'], ' Nodes'
        self.reservation.stop_all()

    def setInstanceConfirmOn(self, instance_id):
        print 'Instance ', instance_id, ' of ', self.node_type, 'confirmed running.'
        self.instances_confirmed += 1
        if self.instances_confirmed == self.num_insts:
            print 'All ', self.num_insts, ' ', self.node_type, 'instances loaded'
            self.ready_for_load_app = True
            self.getServiceNamed('inst_ann_consumer').stopService()
            for i in self.reservation.instances:
                i.update()
            self.public_dns_names = [i.public_dns_name for i in self.reservation.instances]
            self.private_dns_names = [i.private_dns_name for i in self.reservation.instances]
            self.parent.setUnitReadyForLoadApp(self.node_type)

    def setInstacnceConfirmLoaded(self, instance_id):
        print 'Instance ', instance_id, ' of ', self.node_type, 'app loaded.'
        self.apps_loaded += 1
        if self.apps_loaded == self.num_insts:
            print 'all instances of', self.name, ' loaded'
            self.ready_for_config = True
            self.getServiceNamed('load_app_resp_consumer').stopService()
            self.parent.setUnitReadyForConfigApp(self.node_type)

    def setInstacnceConfirmConfiged(self, instance_id):
        print 'Instance ', instance_id, ' of ', self.node_type, 'app configured.'
        self.apps_configed += 1
        if self.apps_configed == self.num_insts:
            print 'all instances of', self.name, ' configured'
            self.ready_for_run = True
            self.getServiceNamed('config_app_resp_consumer').stopService()
            self.parent.setUnitReadyForRunApp(self.node_type)

    def setInstacnceConfirmRunning(self, instance_id):
        print 'Instance ', instance_id, ' of ', self.node_type, 'app running.'
        self.apps_running += 1
        if self.apps_running == self.num_insts:
            print 'all instances of', self.name, ' running!'
            self.getServiceNamed('run_app_resp_consumer').stopService()
            self.status = 'finished'
            self.parent.setUnitFinished(self.node_type)


    def sendDnsNames(self, dns_names):
        """Send a dictionary of all nodes dns names
        """
        self.getServiceNamed('dns_names').operation(dns_names)

    def startLoadApp(self):
        """send command to download and install apps to units who need it
        """
        self.status = 'load app'
        print 'Start Load App for Unit ', self.name
        load_app_script = self.config['load_app_script']
        if load_app_script:
            LoadAppResponseConsumer({'node_type':self.node_type, 'routing_key':self.node_type}).setServiceParent(self)
            self.getServiceNamed('runscript').operation(load_app_script)
        else:
            self.ready_for_config = True
            self.parent.setUnitReadyForConfigApp(self.node_type)



    def startConfigApp(self):
        self.status = 'config app'
        print 'startConfigApp ', self.node_type
        config_app = self.config['config_app']
        if config_app:
            setup_templ = self.config['setup_templ']
            final_setup_path = self.config['final_setup_path']
            f = open(setup_templ)
            script_templ = f.read()
            f.close()
            config_dict = {'config_templ':script_templ, 'path':final_setup_path}
            ConfigAppResponseConsumer({'node_type':self.node_type,'routing_key':self.node_type}).setServiceParent(self)
            self.getServiceNamed('config_templ').operation(config_dict)
        else:
            self.ready_for_run = True
            self.parent.setUnitReadyForRunApp(self.node_type)

    def startRunApp(self):
        self.status = 'run app'
        print 'Run App on node ', self.node_type
        run_app_script = self.config['run_app_script']
        if run_app_script:
            RunAppResponseConsumer({'node_type':self.node_type, 'routing_key':self.node_type}).setServiceParent(self)
            self.getServiceNamed('runscript').operation(run_app_script)


    def get_status_details(self):
        stats = []
        for i in self.reservation.instances:
            inst_stats = []
            inst_stats.append(self.node_type)
            inst_stats.append(self.status)
            inst_stats.append(i.id)
            inst_stats.append(i.public_dns_name)
            stats.append(inst_stats)
        return stats








 


