
from struct import unpack

from twisted.internet import protocol

class BinaryFrameTransceiver(protocol.Protocol):
    """A protocol that sends/receives Frames as defined by the AMQP
    specification.

    """
    FRAME_END = '\xce' # byte marking the end of a frame

    def dataReceived(self, data):
        """
        Incrementally process the incoming data, extracting one frame per
        increment. 
        Ideally, the data will contain an integer number of frames.
        XXX:
        Should remainders be anticipated, or should the connection be
        rebuilt in the event the remaining data does not contain a valid
        frame?
        """
        raw_data = data # keep original bytes around (not utilized though).
        while len(raw_data) > 1:
            frame, raw_data = _extractFrame(raw_data)
            self.frameReceived(frame)

        def _extractFrame(raw_data):
            """
            Extract a frame from the _read_buffer data.
            AMQP Frame Header:

            type (1 byte) | cycle? | channel (2 bytes) | size(4 bytes)
            
            For rabbitmq's 0-8 implementation, cycle seems to be omitted.
            This makes the headers total length 7 bytes, instead of 8.
            """
            _header = raw_data[0:7]
            frame_type, channel, size = unpack('>BHI', _header)
            payload = raw_data[7:size]
            _frame_end = raw_data[7 + size:7 + size + 1]
            if _frame_end != self.FRAME_END:
                raise Exception('Framing Error, received 0x%02x while expecting 0xce' % ord(_frame_end))
            remaining_data = raw_data[7 + size + 1:]
            frame = (frame_type, channel, payload,)
            return frame, remaining_data

    def sendFrame(self, frame):
        """
        Encode frame components, and write binary data to transport.
        The payload should already be encoded.
        @ivar frame: A tuple of the form: (type, channel, payload)
        @type frame: C{tuple}
        """

        self.transport.write(data)

    def frameReceived(self, frame):
        """
        Override this.
        @ivar frame: A tuple of the form: (type, channel, payload)
        @type frame: C{tuple}
        """
        raise NotImplementedError


class AMQPMethodLayer(BinaryFrameTransceiver):
    """
    Method layer receives frames.
    Demultiplex incoming frames by channel number.
    Method layer maintains a list of method consumer queues; one per
    registered consuming channel.

    This might be the connection..
    """
    channels = {}

    def frameReceived(self, frame):
        """
        Transform frame into Method. Combine content-headers and
        content-bodies into a complete Method.
        Route the frame to the destination channel frame consumer queue.
        """


class Demultiplexer(object):
    """
    Method layer pushes method on channel number consumers held here.

    channel method consumer buffers incoming messages and dispatches them
    to their handler asynchronously one at a time.
    """


    def routeMethod(self, method):











