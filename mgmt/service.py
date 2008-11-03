"""
Provisioning management for starting a system in a compute cloud.
The system is comprised of many Units.
A Unit has specific and distinct capability.
A Unit is made of N identical nodes (ami; machine image).

Dorian Raymer 2008
"""
from twisted.internet import defer
from twisted.internet import reactor
from twisted.application import service
from twisted.application import internet

from magnet.agent.service import AMQPService
from magnet.agent.service import Task
from magnet.agent.service import TopicConsumer
from magnet.agent.service import TopicCommandProducer
from magnet.agent.service import read_script_file

import boto

import magnet
magnet_path = magnet.__path__[0]
spec_path = magnet_path + '/amqp0-8.xml'




class Provisioner(service.MultiService):
    """The Service to start all services.
    This triggers commands to be sent to ami's

    """

    units_ready_for_dns = 0
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

    def setUnitReadyForDns(self, unit_name):
        self.units_ready_for_dns += 1
        if self.units_ready_for_dns == self.num_units:
            self.status = 'send dns'
            self.startSendDnsPhase()


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

    def startSendDnsPhase(self):
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
                        provision_exchange='provision',
                        amqp_spec_path=spec_path,
                        aws_access_key=None, 
                        aws_secret_access_key=None):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.broker_vhost = broker_vhost
        self.broker_username = broker_username
        self.broker_password = broker_password
        self.provision_exchange = provision_exchange
        self.amqp_spec_path = spec_path
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.broker_config = {
                        'broker_host':broker_host,
                        'broker_port':broker_port,
                        'broker_vhost':broker_vhost,
                        'broker_username':broker_username,
                        'broker_password':broker_password,
                        'amqp_spec_path':amqp_spec_path,
                        }
        service.MultiService.__init__(self)


    def startService(self):
        ec2 = boto.connect_ec2(self.aws_access_key, self.aws_secret_access_key)

        self.ec2 = ec2
        Provisioner.startService(self)





class ModeSwitchingStatusConsumer(TopicConsumer):

    name = 'status'
    mode = 'setInstacnceConfirmLoaded'

    def set_mode(self, mode):
        self.mode = mode

    def operation(self, *args):
        msg = args[0]
        getattr(self.parent, self.mode)(msg)




class InstanceAnnounceConsumer(TopicConsumer):

    name = 'running'

    def operation(self, *args):
        instance_id = args[0]
        self.parent.setInstanceConfirmOn(instance_id)

class LoadAppResponseConsumer(TopicConsumer):

    name = 'load_app_resp_consumer'

    def operation(self, *args):
        instance_id = args[0]
        self.parent.setInstacnceConfirmLoaded(instance_id)

class ConfigAppResponseConsumer(TopicConsumer):

    name = 'config_app_resp_consumer'

    def operation(self, *args):
        instance_id = args[0]
        self.parent.setInstacnceConfirmConfiged(instance_id)

class RunAppResponseConsumer(TopicConsumer):

    name = 'run_app_resp_consumer'

    def operation(self, *args):
        instance_id = args[0]
        self.parent.setInstacnceConfirmRunning(instance_id)

class ConfigDictCommandProducer(Task):

    name = 'config_dict'
    type = 'produce'

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
    type = 'produce'

    def operation(self, config_templs):
        """config_dict is dict with key/values:
            config_templ:[str of config template]
            path:[path on ami it should be writen]
        """
        msg = str(config_templs)
        self.sendMessage(msg)

class SendAllDnsNames(Task):

    name = 'dns'
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
    apps_dns = 0
    apps_loaded = 0
    apps_configed = 0
    apps_running = 0
    ready_for_app_load = False
    ready_for_config = False
    public_dns_names = []

    def __init__(self, unit_config, broker_config):
        # config = default_AMI_config.update(config)
        self.config = unit_config
        self.broker_config = broker_config
        # broker_config = self.parent.broker_config
        AMQPService.__init__(self, broker_config)
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
        provision_exchange = self.parent.provision_exchange
        user_data = 'node_type='+node_type + ' ' 
        user_data += 'provision_exchange='+provision_exchange + ' '
        user_data += self.config['user-data']
        unit_handler_config = {
                'node_type':node_type,
                'exchange':provision_exchange,
                }
        self.status = 'starting'
        print 'Starting ', N, 'nodes of ', node_type, ami_id
        self.reservation = self.parent.ec2.run_instances(ami_id, min_count=N,
                max_count=N, user_data=user_data)
        # InstanceAnnounceConsumer({'node_type':node_type, 'routing_key':node_type}).setServiceParent(self)
        InstanceAnnounceConsumer(unit_handler_config).setServiceParent(self)
        ModeSwitchingStatusConsumer(unit_handler_config).setServiceParent(self)
        TopicCommandProducer(unit_handler_config).setServiceParent(self)
        SendConfigTemplate(unit_handler_config).setServiceParent(self)
        SendAllDnsNames(unit_handler_config).setServiceParent(self)
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
            self.ready_for_dns = True
            self.getServiceNamed('status').set_mode('setInstacnceConfirmDns')
            for i in self.reservation.instances:
                i.update()
            self.public_dns_names = [i.public_dns_name for i in self.reservation.instances]
            self.private_dns_names = [i.private_dns_name for i in self.reservation.instances]
            self.parent.setUnitReadyForDns(self.node_type)

    def setInstacnceConfirmDns(self, instance_id):
        print 'Instance ', instance_id, ' of ', self.node_type, 'received dns.'
        self.apps_dns += 1
        if self.apps_dns == self.num_insts:
            print 'all instances of', self.name, ' got dns'
            self.ready_for_load_app = True
            self.getServiceNamed('status').set_mode('setInstacnceConfirmLoaded')
            self.parent.setUnitReadyForLoadApp(self.node_type)

    def setInstacnceConfirmLoaded(self, instance_id):
        print 'Instance ', instance_id, ' of ', self.node_type, 'app loaded.'
        self.apps_loaded += 1
        if self.apps_loaded == self.num_insts:
            print 'all instances of', self.name, ' loaded'
            self.ready_for_config = True
            # self.getServiceNamed('load_app_resp_consumer').stopService()
            self.getServiceNamed('status').set_mode('setInstacnceConfirmConfiged')
            self.parent.setUnitReadyForConfigApp(self.node_type)

    def setInstacnceConfirmConfiged(self, instance_id):
        print 'Instance ', instance_id, ' of ', self.node_type, 'app configured.'
        self.apps_configed += 1
        if self.apps_configed == self.num_insts:
            print 'all instances of', self.name, ' configured'
            self.ready_for_run = True
            # self.getServiceNamed('config_app_resp_consumer').stopService()
            self.getServiceNamed('status').set_mode('setInstacnceConfirmRunning')
            self.parent.setUnitReadyForRunApp(self.node_type)

    def setInstacnceConfirmRunning(self, instance_id):
        print 'Instance ', instance_id, ' of ', self.node_type, 'app running.'
        self.apps_running += 1
        if self.apps_running == self.num_insts:
            if self.config.has_key('use_ip'):
                self.reservation.instances[0].use_ip(self.config['use_ip'])
            print 'all instances of', self.name, ' running!'
            # self.getServiceNamed('run_app_resp_consumer').stopService()
            # self.getServiceNamed('status').set_mode()
            self.status = 'finished'
            self.parent.setUnitFinished(self.node_type)


    def sendDnsNames(self, dns_names):
        """Send a dictionary of all nodes dns names
        """
        self.status = 'send dns'
        print 'sending dns names ', self.node_type
        self.getServiceNamed('dns').operation(dns_names)

    def startLoadApp(self):
        """send command to download and install apps to units who need it
        """
        self.status = 'load app'
        print 'Start Load App for Unit ', self.name
        load_app_script = self.config['load_app_script']
        if load_app_script:
            # LoadAppResponseConsumer({'node_type':self.node_type, 'routing_key':self.node_type}).setServiceParent(self)
            self.getServiceNamed('runscript').operation(load_app_script)
        else:
            self.ready_for_config = True
            self.parent.setUnitReadyForConfigApp(self.node_type)



    def startConfigApp(self):
        self.status = 'config app'
        print 'startConfigApp ', self.node_type
        config_app = self.config['config_app']
        if config_app:
            config_templs = [] 
            config_templ_paths = self.config['config_templ_paths']
            for config_templ in config_templ_paths:
                final_setup_path, script_templ = read_config_templ(config_templ)
                config_templs.append([final_setup_path, script_templ])
            # ConfigAppResponseConsumer({'node_type':self.node_type,'routing_key':self.node_type}).setServiceParent(self)
            self.getServiceNamed('config_templ').operation(config_templs)
        else:
            self.ready_for_run = True
            self.parent.setUnitReadyForRunApp(self.node_type)

    def startRunApp(self):
        self.status = 'run app'
        print 'Run App on node ', self.node_type
        run_app_script = self.config['run_app_script']
        if run_app_script:
            # RunAppResponseConsumer({'node_type':self.node_type, 'routing_key':self.node_type}).setServiceParent(self)
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





def read_config_templ(path):
    f = open(path)
    final_setup_path = f.readline()
    script_templ = f.read()
    # the header should have a # then the final path.
    script_templ = script_templ[1:]
    script_templ = script_templ.strip('\n')
    f.close()
    return (final_setup_path, script_templ)



 


