
from twisted.internet import defer
from twisted.internet import reactor
from twisted.application import service
from twisted.application import internet

from magnet.agent.service import AMQPService
from magnet.agent.service import Task
from magnet.agent.service import TopicConsumer
from magnet.agent.service import TopicCommandProducer
from magnet.agent.service import read_script_file





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
            print '%s '*n % tuple(stat)
        print '================================='








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

    def __init__(self, config, ec2):
        # config = default_AMI_config.update(config)
        self.config = config
        AMQPService.__init__(self, config)
        self.ec2 = ec2
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

    def startService(self):
        ami_id = self.config['ami_id']
        N = self.config['num_insts']
        node_type = self.config['node_type']
        user_data = node_type
        self.status = 'starting'
        print 'Starting ', N, 'nodes of ', node_type, ami_id
        self.reservation = self.ec2.run_instances(ami_id, min_count=N,
                max_count=N, user_data=user_data)
        InstanceAnnounceConsumer({'node_type':node_type, 'routing_key':node_type}).setServiceParent(self)
        TopicCommandProducer({'node_type':node_type, 'routing_key':node_type}).setServiceParent(self)
        ConfigDictCommandProducer({'node_type':node_type, 'routing_key':node_type}).setServiceParent(self)
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
        config_app_script = self.config['config_app_script']
        if config_app_script:
            print 'true config app script'
            node_config_dict = self.config['node_config_dict']
            for k,v in node_config_dict.iteritems():
                print k,v
                if v[:4] == 'get_':
                    print v
                    node_to_get = v[4:]
                    new_v = self.parent.getServiceNamed(node_to_get).get_private_dns_name()
                    node_config_dict[k] = new_v
            ConfigAppResponseConsumer({'node_type':self.node_type,'routing_key':self.node_type}).setServiceParent(self)
            # conf_script_temp = read_script_file(config_app_script)
            self.getServiceNamed('config_dict').operation(node_config_dict)
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
            inst_stats.append(i.id)
            inst_stats.append(i.public_dns_name)
            stats.append(inst_stats)
        return stats








 


