"""
The serialization of the messages. 

For now, think of messages as JSON objects; it can grow from here.

"""

import simplejson as json




def serialize_application_message(message_object, serializer='json'):
    serialized_message_object = json.dumps(message_object)
    return serialized_message_object

def unserialize_application_message(serialized_message_object, unserializer='json'):
    message_object = json.loads(serialized_message_object)
    return message_object


class AgentMessage(object):
    """Message format passed between agents
    interaction: or role:
    Control
    Monitor
    Capability
    Contract
    Agent Control
    """
    def __init__(self):
        pass

class RoleMessage(object):
    """Messaeg format passed between actors
    """
    pass
