


class Provisioner(service.Service):
    """The Service to start all services.
    """


class StartAMI(service.Service):

    def __init__(self, ec2, config):
        self.ec2 = ec2
        self.imgid = config['imgid']
        self.N = config['N']
        self.run = None


    def startService(self):
        service.Service.startService(self)
        self.run = self.ec2.run_instances(self.imgid, min_count=self.N,
                max_count=self.N)


