"""
Connection 
 - Implements AMQP Connection Class
 - Has factory for Objects implementing AMQP Channels
 - Is Channel 0 (recieve frames with channel 0)

"""

from twisted.internet.task import coiterate 

from method_framing import MethodReader, MethodWriter

AMQP_0_8 = 'AMQP\x01\x01\x09\x01'
AMQP_PROTOCOL_HEADER = AMQP_0_8

def dispatch_iterator(queue, handler):

    while 1:
        msg = yield queue.get()
        handler(msg)

class AbstractChannelHandler(object):
    """
    Superclass for both the Connection, which is treated
    as channel 0, and other user-created Channel objects.

    The subclasses must have a _METHOD_MAP class property, mapping
    between AMQP method signatures and Python methods.

    """
    def __init__(self, connection, channel_id):
        self.connection = connection
        self.channel_id = channel_id
        connection.channels[channel_id] = self
        self.frame_receive_buffer = defer.DeferredQueue()
        self.method_buffer = defer.DeferredQueue() 
        self.frame_send_buffer = defer.DeferredQueue()

        self.auto_decode = False

        method_reader = MethodReader(self.frame_receive_buffer, self.method_buffer)
        method_writer = MethodWriter(self.frame_send_buffer, connection.frame_max)

        frame_receive_processor = dispatch_iterator(self.frame_receive_buffer, method_reader)
        connection.coop.coiterate(frame_receive_buffer)


    def _send_method(self, method_sig, args='', content=None):
        """
        Send a method for our channel.

        """
        if isinstance(args, AMQPWriter):
            args = args.getvalue()

        self.connection.method_writer.write_method(self.channel_id,
            method_sig, args, content)


    def wait(self, allowed_methods=None):
        """
        Wait for a method that matches our allowed_methods parameter (the
        default value of None means match any method), and dispatch to it.

        Turn this into to iterator/generator
        Once a method is fully formed, it will

        """
        method_sig, args, content = self.connection._wait_method(
            self.channel_id, allowed_methods)

        if content \
        and self.auto_decode \
        and hasattr(content, 'content_encoding'):
            try:
                content.body = content.body.decode(content.content_encoding)
            except:
                pass

        amqp_method = self._METHOD_MAP.get(method_sig, None)

        if amqp_method is None:
            raise Exception('Unknown AMQP method (%d, %d)' % method_sig)

        if content is None:
            return amqp_method(self, args)
        else:
            return amqp_method(self, args, content)


    #
    # Placeholder, the concrete implementations will have to
    # supply their own versions of _METHOD_MAP
    #
    _METHOD_MAP = {}


class ConnectionHandler(AbstractChannelHandler):
    """
    work with socket connections

    The connection class provides methods for a client to establish a
    network connection to a server, and for both peers to operate the
    connection thereafter.

    GRAMMAR:

        connection          = open-connection *use-connection close-connection
        open-connection     = C:protocol-header
                              S:START C:START-OK
                              *challenge
                              S:TUNE C:TUNE-OK
                              C:OPEN S:OPEN-OK | S:REDIRECT
        challenge           = S:SECURE C:SECURE-OK
        use-connection      = *channel
        close-connection    = C:CLOSE S:CLOSE-OK
                            / S:CLOSE C:CLOSE-OK

    """
    def close(self, reply_code, reply_text, class_id, method_id):
        """
        request a connection close

        This method indicates that the sender wants to close the
        connection. This may be due to internal conditions (e.g. a
        forced shut-down) or due to an error handling a specific
        method, i.e. an exception.  When a close is due to an
        exception, the sender provides the class and method id of the
        method which caused the exception.

        RULE:

            After sending this method any received method except the
            Close-OK method MUST be discarded.

        RULE:

            The peer sending this method MAY use a counter or timeout
            to detect failure of the other peer to respond correctly
            with the Close-OK method.

        RULE:

            When a server receives the Close method from a client it
            MUST delete all server-side resources associated with the
            client's context.  A client CANNOT reconnect to a context
            after sending or receiving a Close method.

        PARAMETERS:
            reply_code: short

            reply_text: shortstr

            class_id: short

                failing method class

                When the close is provoked by a method exception, this
                is the class of the method.

            method_id: short

                failing method ID

                When the close is provoked by a method exception, this
                is the ID of the method.

        """
        args = AMQPWriter()
        args.write_short(reply_code)
        args.write_shortstr(reply_text)
        args.write_short(class_id)
        args.write_short(method_id)
        self.send_method_frame(0, (10, 60), args)
        return self.wait(allowed_methods=[
                          (10, 61),    # Connection.close_ok
                        ])


    def _close(self, args):
        """
        request a connection close

        This method indicates that the sender wants to close the
        connection. This may be due to internal conditions (e.g. a
        forced shut-down) or due to an error handling a specific
        method, i.e. an exception.  When a close is due to an
        exception, the sender provides the class and method id of the
        method which caused the exception.

        RULE:

            After sending this method any received method except the
            Close-OK method MUST be discarded.

        RULE:

            The peer sending this method MAY use a counter or timeout
            to detect failure of the other peer to respond correctly
            with the Close-OK method.

        RULE:

            When a server receives the Close method from a client it
            MUST delete all server-side resources associated with the
            client's context.  A client CANNOT reconnect to a context
            after sending or receiving a Close method.

        PARAMETERS:
            reply_code: short

            reply_text: shortstr

            class_id: short

                failing method class

                When the close is provoked by a method exception, this
                is the class of the method.

            method_id: short

                failing method ID

                When the close is provoked by a method exception, this
                is the ID of the method.

        """
        reply_code = args.read_short()
        reply_text = args.read_shortstr()
        class_id = args.read_short()
        method_id = args.read_short()


    def close_ok(self):
        """
        confirm a connection close

        This method confirms a Connection.Close method and tells the
        recipient that it is safe to release resources for the
        connection and close the socket.

        RULE:

            A peer that detects a socket closure without having
            received a Close-Ok handshake method SHOULD log the error.

        """
        self.send_method_frame(0, (10, 61))
        return self.wait(allowed_methods=[
                        ])


    def _close_ok(self, args):
        """
        confirm a connection close

        This method confirms a Connection.Close method and tells the
        recipient that it is safe to release resources for the
        connection and close the socket.

        RULE:

            A peer that detects a socket closure without having
            received a Close-Ok handshake method SHOULD log the error.

        """
        pass


    def open(self, virtual_host, capabilities, insist):
        """
        open connection to virtual host

        This method opens a connection to a virtual host, which is a
        collection of resources, and acts to separate multiple
        application domains within a server.

        RULE:

            The client MUST open the context before doing any work on
            the connection.

        PARAMETERS:
            virtual_host: shortstr

                virtual host name

                The name of the virtual host to work with.

                RULE:

                    If the server supports multiple virtual hosts, it
                    MUST enforce a full separation of exchanges,
                    queues, and all associated entities per virtual
                    host. An application, connected to a specific
                    virtual host, MUST NOT be able to access resources
                    of another virtual host.

                RULE:

                    The server SHOULD verify that the client has
                    permission to access the specified virtual host.

                RULE:

                    The server MAY configure arbitrary limits per
                    virtual host, such as the number of each type of
                    entity that may be used, per connection and/or in
                    total.

            capabilities: shortstr

                required capabilities

                The client may specify a number of capability names,
                delimited by spaces.  The server can use this string
                to how to process the client's connection request.

            insist: bit

                insist on connecting to server

                In a configuration with multiple load-sharing servers,
                the server may respond to a Connection.Open method
                with a Connection.Redirect. The insist option tells
                the server that the client is insisting on a
                connection to the specified server.

                RULE:

                    When the client uses the insist option, the server
                    SHOULD accept the client connection unless it is
                    technically unable to do so.

        """
        args = AMQPWriter()
        args.write_shortstr(virtual_host)
        args.write_shortstr(capabilities)
        args.write_bit(insist)
        self.send_method_frame(0, (10, 40), args)
        return self.wait(allowed_methods=[
                          (10, 41),    # Connection.open_ok
                          (10, 50),    # Connection.redirect
                        ])


    def _open_ok(self, args):
        """
        signal that the connection is ready

        This method signals to the client that the connection is ready
        for use.

        PARAMETERS:
            known_hosts: shortstr

        """
        known_hosts = args.read_shortstr()


    def _redirect(self, args):
        """
        asks the client to use a different server

        This method redirects the client to another server, based on
        the requested virtual host and/or capabilities.

        RULE:

            When getting the Connection.Redirect method, the client
            SHOULD reconnect to the host specified, and if that host
            is not present, to any of the hosts specified in the
            known-hosts list.

        PARAMETERS:
            host: shortstr

                server to connect to

                Specifies the server to connect to.  This is an IP
                address or a DNS name, optionally followed by a colon
                and a port number. If no port number is specified, the
                client should use the default port number for the
                protocol.

            known_hosts: shortstr

        """
        host = args.read_shortstr()
        known_hosts = args.read_shortstr()


    def _secure(self, args):
        """
        security mechanism challenge

        The SASL protocol works by exchanging challenges and responses
        until both peers have received sufficient information to
        authenticate each other.  This method challenges the client to
        provide more information.

        PARAMETERS:
            challenge: longstr

                security challenge data

                Challenge information, a block of opaque binary data
                passed to the security mechanism.

        """
        challenge = args.read_longstr()


    def secure_ok(self, response):
        """
        security mechanism response

        This method attempts to authenticate, passing a block of SASL
        data for the security mechanism at the server side.

        PARAMETERS:
            response: longstr

                security response data

                A block of opaque data passed to the security
                mechanism.  The contents of this data are defined by
                the SASL security mechanism.

        """
        args = AMQPWriter()
        args.write_longstr(response)
        self.send_method_frame(0, (10, 21), args)
        return self.wait(allowed_methods=[
                        ])


    def _start(self, args):
        """
        start connection negotiation

        This method starts the connection negotiation process by
        telling the client the protocol version that the server
        proposes, along with a list of security mechanisms which the
        client can use for authentication.

        RULE:

            If the client cannot handle the protocol version suggested
            by the server it MUST close the socket connection.

        RULE:

            The server MUST provide a protocol version that is lower
            than or equal to that requested by the client in the
            protocol header. If the server cannot support the
            specified protocol it MUST NOT send this method, but MUST
            close the socket connection.

        PARAMETERS:
            version_major: octet

                protocol major version

                The protocol major version that the server agrees to
                use, which cannot be higher than the client's major
                version.

            version_minor: octet

                protocol major version

                The protocol minor version that the server agrees to
                use, which cannot be higher than the client's minor
                version.

            server_properties: table

                server properties

            mechanisms: longstr

                available security mechanisms

                A list of the security mechanisms that the server
                supports, delimited by spaces.  Currently ASL supports
                these mechanisms: PLAIN.

            locales: longstr

                available message locales

                A list of the message locales that the server
                supports, delimited by spaces.  The locale defines the
                language in which the server will send reply texts.

                RULE:

                    All servers MUST support at least the en_US
                    locale.

        """
        version_major = args.read_octet()
        version_minor = args.read_octet()
        server_properties = args.read_table()
        mechanisms = args.read_longstr()
        locales = args.read_longstr()


    def start_ok(self, client_properties, mechanism, response, locale):
        """
        select security mechanism and locale

        This method selects a SASL security mechanism. ASL uses SASL
        (RFC2222) to negotiate authentication and encryption.

        PARAMETERS:
            client_properties: table

                client properties

            mechanism: shortstr

                selected security mechanism

                A single security mechanisms selected by the client,
                which must be one of those specified by the server.

                RULE:

                    The client SHOULD authenticate using the highest-
                    level security profile it can handle from the list
                    provided by the server.

                RULE:

                    The mechanism field MUST contain one of the
                    security mechanisms proposed by the server in the
                    Start method. If it doesn't, the server MUST close
                    the socket.

            response: longstr

                security response data

                A block of opaque data passed to the security
                mechanism. The contents of this data are defined by
                the SASL security mechanism.  For the PLAIN security
                mechanism this is defined as a field table holding two
                fields, LOGIN and PASSWORD.

            locale: shortstr

                selected message locale

                A single message local selected by the client, which
                must be one of those specified by the server.

        """
        args = AMQPWriter()
        args.write_table(client_properties)
        args.write_shortstr(mechanism)
        args.write_longstr(response)
        args.write_shortstr(locale)
        self.send_method_frame(0, (10, 11), args)
        return self.wait(allowed_methods=[
                        ])


    def _tune(self, args):
        """
        propose connection tuning parameters

        This method proposes a set of connection configuration values
        to the client.  The client can accept and/or adjust these.

        PARAMETERS:
            channel_max: short

                proposed maximum channels

                The maximum total number of channels that the server
                allows per connection. Zero means that the server does
                not impose a fixed limit, but the number of allowed
                channels may be limited by available server resources.

            frame_max: long

                proposed maximum frame size

                The largest frame size that the server proposes for
                the connection. The client can negotiate a lower
                value.  Zero means that the server does not impose any
                specific limit but may reject very large frames if it
                cannot allocate resources for them.

                RULE:

                    Until the frame-max has been negotiated, both
                    peers MUST accept frames of up to 4096 octets
                    large. The minimum non-zero value for the frame-
                    max field is 4096.

            heartbeat: short

                desired heartbeat delay

                The delay, in seconds, of the connection heartbeat
                that the server wants.  Zero means the server does not
                want a heartbeat.

        """
        self.channel_max = args.read_short() or self.channel_max
        self.frame_max = args.read_long() or self.frame_max
        self.heartbeat = args.read_short()

        self.tune_ok(self.channel_max, self.frame_max, self.heartbeat)


    def tune_ok(self, channel_max, frame_max, heartbeat):
        """
        negotiate connection tuning parameters

        This method sends the client's connection tuning parameters to
        the server. Certain fields are negotiated, others provide
        capability information.

        PARAMETERS:
            channel_max: short

                negotiated maximum channels

                The maximum total number of channels that the client
                will use per connection.  May not be higher than the
                value specified by the server.

                RULE:

                    The server MAY ignore the channel-max value or MAY
                    use it for tuning its resource allocation.

            frame_max: long

                negotiated maximum frame size

                The largest frame size that the client and server will
                use for the connection.  Zero means that the client
                does not impose any specific limit but may reject very
                large frames if it cannot allocate resources for them.
                Note that the frame-max limit applies principally to
                content frames, where large contents can be broken
                into frames of arbitrary size.

                RULE:

                    Until the frame-max has been negotiated, both
                    peers must accept frames of up to 4096 octets
                    large. The minimum non-zero value for the frame-
                    max field is 4096.

            heartbeat: short

                desired heartbeat delay

                The delay, in seconds, of the connection heartbeat
                that the client wants. Zero means the client does not
                want a heartbeat.

        """
        args = AMQPWriter()
        args.write_short(channel_max)
        args.write_long(frame_max)
        args.write_short(heartbeat)
        self.send_method_frame(0, (10, 31), args)

    _METHOD_MAP = {
        (10, 10): _start,
        (10, 20): _secure,
        (10, 30): _tune,
        (10, 41): _open_ok,
        (10, 50): _redirect,
        (10, 60): _close,
        (10, 61): _close_ok,
        }



class Channel(ChannelHandler):
    """
    """

class Connection(FrameReceiver, ConnectionHandler):
    """
    """

    channel_max = 65535 # 2**16
    frame_max = 4096 # 2**12
    heartbeat = 0 # seconds (0 means no heartbeat)

    _channel = Channel
    _channel_count = 0
    channels = {}

    def __init__(self):
        """
        """
        self.frame_receive_buffer = defer.DeferredQueue()
        self.transmit_buffer = defer.DeferredQueue()
        ConnectionHandler(self, connection, 0)
        self.coop = task.Cooperator()

    def connectionMade(self):
        """
        Send protocol header and activate channel 0 connection class
        handler
        """
        self.transport.write(AMQP_PROTOCOL_HEADER)
        self.coop.start()

    def frameReceived(self, frame):
        """
        """
        chan_num = frame[1]
        try:
            chan = self.channels.get(chan_num)
            chan.frame_buffer.put(frame)
        except KeyError:
            # log
            return 
        finally:
            return


    def channel(self):
        """create new channel and return it
        """
        chan_id = self._next_channel_id()
        c = self._channel(self, chan_id)
        self.channels[num] = c
        return c

    def _next_channel_id(self):
        self._channel_count += 1
        return self._channel_count
