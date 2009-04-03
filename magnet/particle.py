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


def message():
    """create a message with a certain structure
    """
