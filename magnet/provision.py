"""
"""




class EC2Configuration(usage.Options):

    optParameters = [
            ("image-id", None, None, "AMI ID",),
            ]


class Phase(object):
    """
    Phase holds the state information a  Pole can operate on
    """

    def __init__(self, phase_control):
        self.phase_control = phase_control


    def start_phase(self):
        """do something when the phase starts
        """

class StatePole(pole.BasePole):
    """
    This represents an application on a machine.
    When the Main Pole is running, it starts each of the Poles that have
    been registered with it. 
    The Main Pole is responsible for general machine
    configuration/conceptualization, and the child Poles are
    responsible for their applications.

    Actions should be written in pairs. One action sets the poles current
    phase, and sends a message to the scheduler announcing it's phase,
    asking for what it needs, etc.
    The second action in the pair waits to receive an reply from the
    scheduler. A periodic monitor supervises the Poles Phase changes and
    makes sure it doesn't get stuck in a bad Phase. 
    If the scheduler isn't prepared to send the Pole what it needs, it will
    respond telling it to wait for more messages in that Phase.

    An application pole should have a standard interface for controlling
    the application process. The standard methods should be the place
    specific application commands are implemented.

    The main pole should be able to ask it's child poles what phase they
    are in. The main pole knows what child poles it has because the child
    poles are registered with the main pole.

    It should be possible to command the main pole to completely start/stop
    an application simply by starting/stopping the application pole.

    Phases keep track of what is needed now and what can happen now.

    Messages are only handled by poles; phase can determine/effect how a
    message is handled.


    """

    phases = (
            (0, 'Startup'),
            (1, 'Configuration'),
            (2, 'Ready'),
            (3, 'Running'),
            )
    current_phase = 0


    def startService(self):
        self.phase = self.phases[0]

    def stopService(self):
        """depending on phase, turn everything necessary off
        """

    def start_phase(self, phase):
        p = getattr(self, 'phase_%s', % str(phase))
        p()

    def request_configuration(self):
        """this sends a message to the scheduler asking for all the
        variables it needs to move out of the configuration state.

        The configuration phase will invoke this function.
        If there is no response, it will ask again periodically.
        """

    def receive_configuration(self, message_object):
        """this handles messages from the mapper/scheduler that have configuration
        information.

        If the phase is 'configuration' this configuration will be applied
        to the system.

        Message could be: 'wait longer' if the scheduler needs something
        else to happen.
        """



    def start_application(self, message_object):
        """a trigger function to start an application. The pole should
        already know how to do this. 
        
        Check that the state of the other phases satisfies the requirements
        for this phase to start.

        Report success or failure to the mapper, set phase accordingly.
        """

    def stop_application(self, message_object):
        """There may be many ways to stop an application...
        """

    def test_application(self, message_object):
        """run test of application availability/functionality.
        This can only be run if the phase is 'application running'
        return 
            - application not running (wrong phase, not necessarily an
              error)
            - test success
            - test fail, reason, report

        """









