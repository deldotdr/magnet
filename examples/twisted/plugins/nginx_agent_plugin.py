
from twisted.internet.utils import getProcessOutput

from magnet import pole


class NginxControl(pole.BasePole):
    """
    Experimental Nginx control agent.
    getProcessOutput uses sudo, so sudo must be configured to not use
    password for the user that runs this.
    """

    def __repr__(self):
        return "Nginx Control Agent"

    def action_start(self, message_object):
        """
        """
        d = getProcessOutput('sudo /etc/init.d/nginx', args=('start',))
        return d

    def action_stop(self, message_object):
        """
        """
        d = getProcessOutput('sudo /etc/init.d/nginx', args=('stop',))
        return d

    def action_restart(self, message_object):
        """
        """
        d = getProcessOutput('sudo /etc/init.d/nginx', args=('restart',))
        return d

    def action_reload(self, message_object):
        """
        """
        d = getProcessOutput('sudo /etc/init.d/nginx', args=('reload',))
        return d

# nginxcontrol = NginxControl('magnet', 'test', 'test', token='nginx')
