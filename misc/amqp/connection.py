"""
Connection 
 - Implements AMQP Connection Class
 - Has factory for Objects implementing AMQP Channels
 - Is Channel 0 (recieve frames with channel 0)

"""
from struct import unpack, pack

from twisted.internet import protocol
from twisted.internet import defer
from twisted.internet import task 

from method_framing import MethodReader, MethodWriter
from serialization import AMQPWriter
from exceptions import *

AMQP_0_8 = 'AMQP\x01\x01\x09\x01'
AMQP_PROTOCOL_HEADER = AMQP_0_8

LIBRARY_PROPERTIES = {
        'library': 'Dynamo amqplib',
        'library_version': '0.0',
        }

def dispatch_iterator(queue):
    while 1:
        yield queue.get()

def buffer_iterator(buf):
    while 1:
        yield buf.next()

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
        self.frame_send_buffer = connection.frame_send_buffer

        self.auto_decode = False

        method_reader = MethodReader(self.frame_receive_buffer, self.method_buffer)
        method_writer = MethodWriter(self.frame_send_buffer, connection.frame_max)
        self.method_writer = method_writer

        self.method_buffer.get().addCallback(self.wait)

        method_reader_iter = buffer_iterator(method_reader)
        # method_writer_iter = buffer_iterator(method_writer)
        method_reader_defer = connection.coop.coiterate(method_reader_iter)
        # method_writer_defer = connection.coop.coiterate(method_writer_iter)


    def _send_method(self, method_sig, args='', content=None):
        """
        Send a method for our channel.

        """
        if isinstance(args, AMQPWriter):
            args = args.getvalue()

        self.method_writer.write_method(self.channel_id,
            method_sig, args, content)


    def wait(self, method):
        """
        Wait for a method that matches our allowed_methods parameter (the
        default value of None means match any method), and dispatch to it.

        Turn this into to iterator/generator
        Once a method is fully formed, it will

        """
        # method_sig, args, content = self.connection._wait_method( self.channel_id, allowed_methods)
        chan, method_sig, args, content = method

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
            # return amqp_method(self, args)
            amqp_method(self, args)
        else:
            amqp_method(self, args, content)
        self.method_buffer.get().addCallback(self.wait)


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
        mechanisms = args.read_longstr().split(' ')
        locales = args.read_longstr().split(' ')
        self.start_ok(LIBRARY_PROPERTIES, 'AMQPLAIN', self.login, locales[0])


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
        self._send_method((10, 11), args)


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
        self._send_method((10, 31), args)

    _METHOD_MAP = {
        (10, 10): _start,
        (10, 20): _secure,
        (10, 30): _tune,
        (10, 41): _open_ok,
        (10, 50): _redirect,
        (10, 60): _close,
        (10, 61): _close_ok,
        }


class Channel(AbstractChannelHandler):
    """
    work with channels

    The channel class provides methods for a client to establish a
    virtual connection - a channel - to a server and for both peers to
    operate the virtual connection thereafter.

    GRAMMAR:

        channel             = open-channel *use-channel close-channel
        open-channel        = C:OPEN S:OPEN-OK
        use-channel         = C:FLOW S:FLOW-OK
                            / S:FLOW C:FLOW-OK
                            / S:ALERT
                            / functional-class
        close-channel       = C:CLOSE S:CLOSE-OK
                            / S:CLOSE C:CLOSE-OK

    """
    def _alert(self, args):
        """
        send a non-fatal warning message

        This method allows the server to send a non-fatal warning to
        the client.  This is used for methods that are normally
        asynchronous and thus do not have confirmations, and for which
        the server may detect errors that need to be reported.  Fatal
        errors are handled as channel or connection exceptions; non-
        fatal errors are sent through this method.

        PARAMETERS:
            reply_code: short

            reply_text: shortstr

            details: table

                detailed information for warning

                A set of fields that provide more information about
                the problem.  The meaning of these fields are defined
                on a per-reply-code basis (TO BE DEFINED).

        """
        reply_code = args.read_short()
        reply_text = args.read_shortstr()
        details = args.read_table()


    def close(self, reply_code, reply_text, class_id, method_id):
        """
        request a channel close

        This method indicates that the sender wants to close the
        channel. This may be due to internal conditions (e.g. a forced
        shut-down) or due to an error handling a specific method, i.e.
        an exception.  When a close is due to an exception, the sender
        provides the class and method id of the method which caused
        the exception.

        RULE:

            After sending this method any received method except
            Channel.Close-OK MUST be discarded.

        RULE:

            The peer sending this method MAY use a counter or timeout
            to detect failure of the other peer to respond correctly
            with Channel.Close-OK..

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
        self.send_method_frame((20, 40), args)
        return self.wait(allowed_methods=[
                          (20, 41),    # Channel.close_ok
                        ])


    def _close(self, args):
        """
        request a channel close

        This method indicates that the sender wants to close the
        channel. This may be due to internal conditions (e.g. a forced
        shut-down) or due to an error handling a specific method, i.e.
        an exception.  When a close is due to an exception, the sender
        provides the class and method id of the method which caused
        the exception.

        RULE:

            After sending this method any received method except
            Channel.Close-OK MUST be discarded.

        RULE:

            The peer sending this method MAY use a counter or timeout
            to detect failure of the other peer to respond correctly
            with Channel.Close-OK..

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
        confirm a channel close

        This method confirms a Channel.Close method and tells the
        recipient that it is safe to release resources for the channel
        and close the socket.

        RULE:

            A peer that detects a socket closure without having
            received a Channel.Close-Ok handshake method SHOULD log
            the error.

        """
        self.send_method_frame((20, 41))
        return self.wait(allowed_methods=[
                        ])


    def _close_ok(self, args):
        """
        confirm a channel close

        This method confirms a Channel.Close method and tells the
        recipient that it is safe to release resources for the channel
        and close the socket.

        RULE:

            A peer that detects a socket closure without having
            received a Channel.Close-Ok handshake method SHOULD log
            the error.

        """
        pass


    def flow(self, active):
        """
        enable/disable flow from peer

        This method asks the peer to pause or restart the flow of
        content data. This is a simple flow-control mechanism that a
        peer can use to avoid oveflowing its queues or otherwise
        finding itself receiving more messages than it can process.
        Note that this method is not intended for window control.  The
        peer that receives a request to stop sending content should
        finish sending the current content, if any, and then wait
        until it receives a Flow restart method.

        RULE:

            When a new channel is opened, it is active.  Some
            applications assume that channels are inactive until
            started.  To emulate this behaviour a client MAY open the
            channel, then pause it.

        RULE:

            When sending content data in multiple frames, a peer
            SHOULD monitor the channel for incoming methods and
            respond to a Channel.Flow as rapidly as possible.

        RULE:

            A peer MAY use the Channel.Flow method to throttle
            incoming content data for internal reasons, for example,
            when exchangeing data over a slower connection.

        RULE:

            The peer that requests a Channel.Flow method MAY
            disconnect and/or ban a peer that does not respect the
            request.

        PARAMETERS:
            active: bit

                start/stop content frames

                If 1, the peer starts sending content frames.  If 0,
                the peer stops sending content frames.

        """
        args = AMQPWriter()
        args.write_bit(active)
        self.send_method_frame((20, 20), args)
        return self.wait(allowed_methods=[
                          (20, 21),    # Channel.flow_ok
                        ])


    def _flow(self, args):
        """
        enable/disable flow from peer

        This method asks the peer to pause or restart the flow of
        content data. This is a simple flow-control mechanism that a
        peer can use to avoid oveflowing its queues or otherwise
        finding itself receiving more messages than it can process.
        Note that this method is not intended for window control.  The
        peer that receives a request to stop sending content should
        finish sending the current content, if any, and then wait
        until it receives a Flow restart method.

        RULE:

            When a new channel is opened, it is active.  Some
            applications assume that channels are inactive until
            started.  To emulate this behaviour a client MAY open the
            channel, then pause it.

        RULE:

            When sending content data in multiple frames, a peer
            SHOULD monitor the channel for incoming methods and
            respond to a Channel.Flow as rapidly as possible.

        RULE:

            A peer MAY use the Channel.Flow method to throttle
            incoming content data for internal reasons, for example,
            when exchangeing data over a slower connection.

        RULE:

            The peer that requests a Channel.Flow method MAY
            disconnect and/or ban a peer that does not respect the
            request.

        PARAMETERS:
            active: bit

                start/stop content frames

                If 1, the peer starts sending content frames.  If 0,
                the peer stops sending content frames.

        """
        active = args.read_bit()


    def flow_ok(self, active):
        """
        confirm a flow method

        Confirms to the peer that a flow command was received and
        processed.

        PARAMETERS:
            active: bit

                current flow setting

                Confirms the setting of the processed flow method: 1
                means the peer will start sending or continue to send
                content frames; 0 means it will not.

        """
        args = AMQPWriter()
        args.write_bit(active)
        self.send_method_frame((20, 21), args)


    def _flow_ok(self, args):
        """
        confirm a flow method

        Confirms to the peer that a flow command was received and
        processed.

        PARAMETERS:
            active: bit

                current flow setting

                Confirms the setting of the processed flow method: 1
                means the peer will start sending or continue to send
                content frames; 0 means it will not.

        """
        active = args.read_bit()


    def open(self, out_of_band):
        """
        open a channel for use

        This method opens a virtual connection (a channel).

        RULE:

            This method MUST NOT be called when the channel is already
            open.

        PARAMETERS:
            out_of_band: shortstr

                out-of-band settings

                Configures out-of-band transfers on this channel.  The
                syntax and meaning of this field will be formally
                defined at a later date.

        """
        args = AMQPWriter()
        args.write_shortstr(out_of_band)
        self._send_method((20, 10), args)


    def _open_ok(self, args):
        """
        signal that the channel is ready

        This method signals to the client that the channel is ready
        for use.

        """
        pass


    #############
    #
    #  Access
    #
    #
    # work with access tickets
    #
    # The protocol control access to server resources using access
    # tickets. A client must explicitly request access tickets before
    # doing work. An access ticket grants a client the right to use a
    # specific set of resources - called a "realm" - in specific ways.
    #
    # GRAMMAR:
    #
    #     access              = C:REQUEST S:REQUEST-OK
    #
    #

    def access_request(self, realm, exclusive, passive, active, write, read):
        """
        request an access ticket

        This method requests an access ticket for an access realm. The
        server responds by granting the access ticket.  If the client
        does not have access rights to the requested realm this causes
        a connection exception.  Access tickets are a per-channel
        resource.

        RULE:

            The realm name MUST start with either "/data" (for
            application resources) or "/admin" (for server
            administration resources). If the realm starts with any
            other path, the server MUST raise a connection exception
            with reply code 403 (access refused).

        RULE:

            The server MUST implement the /data realm and MAY
            implement the /admin realm.  The mapping of resources to
            realms is not defined in the protocol - this is a server-
            side configuration issue.

        PARAMETERS:
            realm: shortstr

                name of requested realm

                RULE:

                    If the specified realm is not known to the server,
                    the server must raise a channel exception with
                    reply code 402 (invalid path).

            exclusive: bit

                request exclusive access

                Request exclusive access to the realm. If the server
                cannot grant this - because there are other active
                tickets for the realm - it raises a channel exception.

            passive: bit

                request passive access

                Request message passive access to the specified access
                realm. Passive access lets a client get information
                about resources in the realm but not to make any
                changes to them.

            active: bit

                request active access

                Request message active access to the specified access
                realm. Acvtive access lets a client get create and
                delete resources in the realm.

            write: bit

                request write access

                Request write access to the specified access realm.
                Write access lets a client publish messages to all
                exchanges in the realm.

            read: bit

                request read access

                Request read access to the specified access realm.
                Read access lets a client consume messages from queues
                in the realm.

        """
        args = AMQPWriter()
        args.write_shortstr(realm)
        args.write_bit(exclusive)
        args.write_bit(passive)
        args.write_bit(active)
        args.write_bit(write)
        args.write_bit(read)
        self.send_method_frame((30, 10), args)
        return self.wait(allowed_methods=[
                          (30, 11),    # Channel.access_request_ok
                        ])


    def _access_request_ok(self, args):
        """
        grant access to server resources

        This method provides the client with an access ticket. The
        access ticket is valid within the current channel and for the
        lifespan of the channel.

        RULE:

            The client MUST NOT use access tickets except within the
            same channel as originally granted.

        RULE:

            The server MUST isolate access tickets per channel and
            treat an attempt by a client to mix these as a connection
            exception.

        PARAMETERS:
            ticket: short

        """
        ticket = args.read_short()


    #############
    #
    #  Exchange
    #
    #
    # work with exchanges
    #
    # Exchanges match and distribute messages across queues.
    # Exchanges can be configured in the server or created at runtime.
    #
    # GRAMMAR:
    #
    #     exchange            = C:DECLARE  S:DECLARE-OK
    #                         / C:DELETE   S:DELETE-OK
    #
    # RULE:
    #
    #     The server MUST implement the direct and fanout exchange
    #     types, and predeclare the corresponding exchanges named
    #     amq.direct and amq.fanout in each virtual host. The server
    #     MUST also predeclare a direct exchange to act as the default
    #     exchange for content Publish methods and for default queue
    #     bindings.
    #
    # RULE:
    #
    #     The server SHOULD implement the topic exchange type, and
    #     predeclare the corresponding exchange named amq.topic in
    #     each virtual host.
    #
    # RULE:
    #
    #     The server MAY implement the system exchange type, and
    #     predeclare the corresponding exchanges named amq.system in
    #     each virtual host. If the client attempts to bind a queue to
    #     the system exchange, the server MUST raise a connection
    #     exception with reply code 507 (not allowed).
    #
    # RULE:
    #
    #     The default exchange MUST be defined as internal, and be
    #     inaccessible to the client except by specifying an empty
    #     exchange name in a content Publish method. That is, the
    #     server MUST NOT let clients make explicit bindings to this
    #     exchange.
    #
    #

    def exchange_declare(self, exchange, type, passive, durable, auto_delete, internal, nowait, arguments, ticket):
        """
        declare exchange, create if needed

        This method creates an exchange if it does not already exist,
        and if the exchange exists, verifies that it is of the correct
        and expected class.

        RULE:

            The server SHOULD support a minimum of 16 exchanges per
            virtual host and ideally, impose no limit except as
            defined by available resources.

        PARAMETERS:
            ticket: short

                When a client defines a new exchange, this belongs to
                the access realm of the ticket used.  All further work
                done with that exchange must be done with an access
                ticket for the same realm.

                RULE:

                    The client MUST provide a valid access ticket
                    giving "active" access to the realm in which the
                    exchange exists or will be created, or "passive"
                    access if the if-exists flag is set.

            exchange: shortstr

                RULE:

                    Exchange names starting with "amq." are reserved
                    for predeclared and standardised exchanges.  If
                    the client attempts to create an exchange starting
                    with "amq.", the server MUST raise a channel
                    exception with reply code 403 (access refused).

            type: shortstr

                exchange type

                Each exchange belongs to one of a set of exchange
                types implemented by the server.  The exchange types
                define the functionality of the exchange - i.e. how
                messages are routed through it.  It is not valid or
                meaningful to attempt to change the type of an
                existing exchange.

                RULE:

                    If the exchange already exists with a different
                    type, the server MUST raise a connection exception
                    with a reply code 507 (not allowed).

                RULE:

                    If the server does not support the requested
                    exchange type it MUST raise a connection exception
                    with a reply code 503 (command invalid).

            passive: bit

                do not create exchange

                If set, the server will not create the exchange.  The
                client can use this to check whether an exchange
                exists without modifying the server state.

                RULE:

                    If set, and the exchange does not already exist,
                    the server MUST raise a channel exception with
                    reply code 404 (not found).

            durable: bit

                request a durable exchange

                If set when creating a new exchange, the exchange will
                be marked as durable.  Durable exchanges remain active
                when a server restarts. Non-durable exchanges
                (transient exchanges) are purged if/when a server
                restarts.

                RULE:

                    The server MUST support both durable and transient
                    exchanges.

                RULE:

                    The server MUST ignore the durable field if the
                    exchange already exists.

            auto_delete: bit

                auto-delete when unused

                If set, the exchange is deleted when all queues have
                finished using it.

                RULE:

                    The server SHOULD allow for a reasonable delay
                    between the point when it determines that an
                    exchange is not being used (or no longer used),
                    and the point when it deletes the exchange.  At
                    the least it must allow a client to create an
                    exchange and then bind a queue to it, with a small
                    but non-zero delay between these two actions.

                RULE:

                    The server MUST ignore the auto-delete field if
                    the exchange already exists.

            internal: bit

                create internal exchange

                If set, the exchange may not be used directly by
                publishers, but only when bound to other exchanges.
                Internal exchanges are used to construct wiring that
                is not visible to applications.

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

            arguments: table

                arguments for declaration

                A set of arguments for the declaration. The syntax and
                semantics of these arguments depends on the server
                implementation.  This field is ignored if passive is
                1.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(exchange)
        args.write_shortstr(type)
        args.write_bit(passive)
        args.write_bit(durable)
        args.write_bit(auto_delete)
        args.write_bit(internal)
        args.write_bit(nowait)
        args.write_table(arguments)
        self.send_method_frame((40, 10), args)
        return self.wait(allowed_methods=[
                          (40, 11),    # Channel.exchange_declare_ok
                        ])


    def _exchange_declare_ok(self, args):
        """
        confirms an exchange declaration

        This method confirms a Declare method and confirms the name of
        the exchange, essential for automatically-named exchanges.

        """
        pass


    def exchange_delete(self, exchange, if_unused, nowait, ticket):
        """
        delete an exchange

        This method deletes an exchange.  When an exchange is deleted
        all queue bindings on the exchange are cancelled.

        PARAMETERS:
            ticket: short

                RULE:

                    The client MUST provide a valid access ticket
                    giving "active" access rights to the exchange's
                    access realm.

            exchange: shortstr

                RULE:

                    The exchange MUST exist. Attempting to delete a
                    non-existing exchange causes a channel exception.

            if_unused: bit

                delete only if unused

                If set, the server will only delete the exchange if it
                has no queue bindings. If the exchange has queue
                bindings the server does not delete it but raises a
                channel exception instead.

                RULE:

                    If set, the server SHOULD delete the exchange but
                    only if it has no queue bindings.

                RULE:

                    If set, the server SHOULD raise a channel
                    exception if the exchange is in use.

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(exchange)
        args.write_bit(if_unused)
        args.write_bit(nowait)
        self.send_method_frame((40, 20), args)
        return self.wait(allowed_methods=[
                          (40, 21),    # Channel.exchange_delete_ok
                        ])


    def _exchange_delete_ok(self, args):
        """
        confirm deletion of an exchange

        This method confirms the deletion of an exchange.

        """
        pass


    #############
    #
    #  Queue
    #
    #
    # work with queues
    #
    # Queues store and forward messages.  Queues can be configured in
    # the server or created at runtime.  Queues must be attached to at
    # least one exchange in order to receive messages from publishers.
    #
    # GRAMMAR:
    #
    #     queue               = C:DECLARE  S:DECLARE-OK
    #                         / C:BIND     S:BIND-OK
    #                         / C:PURGE    S:PURGE-OK
    #                         / C:DELETE   S:DELETE-OK
    #
    # RULE:
    #
    #     A server MUST allow any content class to be sent to any
    #     queue, in any mix, and queue and delivery these content
    #     classes independently. Note that all methods that fetch
    #     content off queues are specific to a given content class.
    #
    #

    def queue_bind(self, queue, exchange, routing_key, nowait, arguments, ticket):
        """
        bind queue to an exchange

        This method binds a queue to an exchange.  Until a queue is
        bound it will not receive any messages.  In a classic
        messaging model, store-and-forward queues are bound to a dest
        exchange and subscription queues are bound to a dest_wild
        exchange.

        RULE:

            A server MUST allow ignore duplicate bindings - that is,
            two or more bind methods for a specific queue, with
            identical arguments - without treating these as an error.

        RULE:

            If a bind fails, the server MUST raise a connection
            exception.

        RULE:

            The server MUST NOT allow a durable queue to bind to a
            transient exchange. If the client attempts this the server
            MUST raise a channel exception.

        RULE:

            Bindings for durable queues are automatically durable and
            the server SHOULD restore such bindings after a server
            restart.

        RULE:

            If the client attempts to an exchange that was declared as
            internal, the server MUST raise a connection exception
            with reply code 530 (not allowed).

        RULE:

            The server SHOULD support at least 4 bindings per queue,
            and ideally, impose no limit except as defined by
            available resources.

        PARAMETERS:
            ticket: short

                The client provides a valid access ticket giving
                "active" access rights to the queue's access realm.

            queue: shortstr

                Specifies the name of the queue to bind.  If the queue
                name is empty, refers to the current queue for the
                channel, which is the last declared queue.

                RULE:

                    If the client did not previously declare a queue,
                    and the queue name in this method is empty, the
                    server MUST raise a connection exception with
                    reply code 530 (not allowed).

                RULE:

                    If the queue does not exist the server MUST raise
                    a channel exception with reply code 404 (not
                    found).

            exchange: shortstr

                The name of the exchange to bind to.

                RULE:

                    If the exchange does not exist the server MUST
                    raise a channel exception with reply code 404 (not
                    found).

            routing_key: shortstr

                message routing key

                Specifies the routing key for the binding.  The
                routing key is used for routing messages depending on
                the exchange configuration. Not all exchanges use a
                routing key - refer to the specific exchange
                documentation.  If the routing key is empty and the
                queue name is empty, the routing key will be the
                current queue for the channel, which is the last
                declared queue.

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

            arguments: table

                arguments for binding

                A set of arguments for the binding.  The syntax and
                semantics of these arguments depends on the exchange
                class.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(queue)
        args.write_shortstr(exchange)
        args.write_shortstr(routing_key)
        args.write_bit(nowait)
        args.write_table(arguments)
        self.send_method_frame((50, 20), args)
        return self.wait(allowed_methods=[
                          (50, 21),    # Channel.queue_bind_ok
                        ])


    def _queue_bind_ok(self, args):
        """
        confirm bind successful

        This method confirms that the bind was successful.

        """
        pass


    def queue_declare(self, queue, passive, durable, exclusive, auto_delete, nowait, arguments, ticket):
        """
        declare queue, create if needed

        This method creates or checks a queue.  When creating a new
        queue the client can specify various properties that control
        the durability of the queue and its contents, and the level of
        sharing for the queue.

        RULE:

            The server MUST create a default binding for a newly-
            created queue to the default exchange, which is an
            exchange of type 'direct'.

        RULE:

            The server SHOULD support a minimum of 256 queues per
            virtual host and ideally, impose no limit except as
            defined by available resources.

        PARAMETERS:
            ticket: short

                When a client defines a new queue, this belongs to the
                access realm of the ticket used.  All further work
                done with that queue must be done with an access
                ticket for the same realm.

                The client provides a valid access ticket giving
                "active" access to the realm in which the queue exists
                or will be created, or "passive" access if the if-
                exists flag is set.

            queue: shortstr

                RULE:

                    The queue name MAY be empty, in which case the
                    server MUST create a new queue with a unique
                    generated name and return this to the client in
                    the Declare-Ok method.

                RULE:

                    Queue names starting with "amq." are reserved for
                    predeclared and standardised server queues.  If
                    the queue name starts with "amq." and the passive
                    option is zero, the server MUST raise a connection
                    exception with reply code 403 (access refused).

            passive: bit

                do not create queue

                If set, the server will not create the queue.  The
                client can use this to check whether a queue exists
                without modifying the server state.

                RULE:

                    If set, and the queue does not already exist, the
                    server MUST respond with a reply code 404 (not
                    found) and raise a channel exception.

            durable: bit

                request a durable queue

                If set when creating a new queue, the queue will be
                marked as durable.  Durable queues remain active when
                a server restarts. Non-durable queues (transient
                queues) are purged if/when a server restarts.  Note
                that durable queues do not necessarily hold persistent
                messages, although it does not make sense to send
                persistent messages to a transient queue.

                RULE:

                    The server MUST recreate the durable queue after a
                    restart.

                RULE:

                    The server MUST support both durable and transient
                    queues.

                RULE:

                    The server MUST ignore the durable field if the
                    queue already exists.

            exclusive: bit

                request an exclusive queue

                Exclusive queues may only be consumed from by the
                current connection. Setting the 'exclusive' flag
                always implies 'auto-delete'.

                RULE:

                    The server MUST support both exclusive (private)
                    and non-exclusive (shared) queues.

                RULE:

                    The server MUST raise a channel exception if
                    'exclusive' is specified and the queue already
                    exists and is owned by a different connection.

            auto_delete: bit

                auto-delete queue when unused

                If set, the queue is deleted when all consumers have
                finished using it. Last consumer can be cancelled
                either explicitly or because its channel is closed. If
                there was no consumer ever on the queue, it won't be
                deleted.

                RULE:

                    The server SHOULD allow for a reasonable delay
                    between the point when it determines that a queue
                    is not being used (or no longer used), and the
                    point when it deletes the queue.  At the least it
                    must allow a client to create a queue and then
                    create a consumer to read from it, with a small
                    but non-zero delay between these two actions.  The
                    server should equally allow for clients that may
                    be disconnected prematurely, and wish to re-
                    consume from the same queue without losing
                    messages.  We would recommend a configurable
                    timeout, with a suitable default value being one
                    minute.

                RULE:

                    The server MUST ignore the auto-delete field if
                    the queue already exists.

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

            arguments: table

                arguments for declaration

                A set of arguments for the declaration. The syntax and
                semantics of these arguments depends on the server
                implementation.  This field is ignored if passive is
                1.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(queue)
        args.write_bit(passive)
        args.write_bit(durable)
        args.write_bit(exclusive)
        args.write_bit(auto_delete)
        args.write_bit(nowait)
        args.write_table(arguments)
        self.send_method_frame((50, 10), args)
        return self.wait(allowed_methods=[
                          (50, 11),    # Channel.queue_declare_ok
                        ])


    def _queue_declare_ok(self, args):
        """
        confirms a queue definition

        This method confirms a Declare method and confirms the name of
        the queue, essential for automatically-named queues.

        PARAMETERS:
            queue: shortstr

                Reports the name of the queue. If the server generated
                a queue name, this field contains that name.

            message_count: long

                number of messages in queue

                Reports the number of messages in the queue, which
                will be zero for newly-created queues.

            consumer_count: long

                number of consumers

                Reports the number of active consumers for the queue.
                Note that consumers can suspend activity
                (Channel.Flow) in which case they do not appear in
                this count.

        """
        queue = args.read_shortstr()
        message_count = args.read_long()
        consumer_count = args.read_long()


    def queue_delete(self, queue, if_unused, if_empty, nowait, ticket):
        """
        delete a queue

        This method deletes a queue.  When a queue is deleted any
        pending messages are sent to a dead-letter queue if this is
        defined in the server configuration, and all consumers on the
        queue are cancelled.

        RULE:

            The server SHOULD use a dead-letter queue to hold messages
            that were pending on a deleted queue, and MAY provide
            facilities for a system administrator to move these
            messages back to an active queue.

        PARAMETERS:
            ticket: short

                The client provides a valid access ticket giving
                "active" access rights to the queue's access realm.

            queue: shortstr

                Specifies the name of the queue to delete. If the
                queue name is empty, refers to the current queue for
                the channel, which is the last declared queue.

                RULE:

                    If the client did not previously declare a queue,
                    and the queue name in this method is empty, the
                    server MUST raise a connection exception with
                    reply code 530 (not allowed).

                RULE:

                    The queue must exist. Attempting to delete a non-
                    existing queue causes a channel exception.

            if_unused: bit

                delete only if unused

                If set, the server will only delete the queue if it
                has no consumers. If the queue has consumers the
                server does does not delete it but raises a channel
                exception instead.

                RULE:

                    The server MUST respect the if-unused flag when
                    deleting a queue.

            if_empty: bit

                delete only if empty

                If set, the server will only delete the queue if it
                has no messages. If the queue is not empty the server
                raises a channel exception.

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(queue)
        args.write_bit(if_unused)
        args.write_bit(if_empty)
        args.write_bit(nowait)
        self.send_method_frame((50, 40), args)
        return self.wait(allowed_methods=[
                          (50, 41),    # Channel.queue_delete_ok
                        ])


    def _queue_delete_ok(self, args):
        """
        confirm deletion of a queue

        This method confirms the deletion of a queue.

        PARAMETERS:
            message_count: long

                number of messages purged

                Reports the number of messages purged.

        """
        message_count = args.read_long()


    def queue_purge(self, queue, nowait, ticket):
        """
        purge a queue

        This method removes all messages from a queue.  It does not
        cancel consumers.  Purged messages are deleted without any
        formal "undo" mechanism.

        RULE:

            A call to purge MUST result in an empty queue.

        RULE:

            On transacted channels the server MUST not purge messages
            that have already been sent to a client but not yet
            acknowledged.

        RULE:

            The server MAY implement a purge queue or log that allows
            system administrators to recover accidentally-purged
            messages.  The server SHOULD NOT keep purged messages in
            the same storage spaces as the live messages since the
            volumes of purged messages may get very large.

        PARAMETERS:
            ticket: short

                The access ticket must be for the access realm that
                holds the queue.

                RULE:

                    The client MUST provide a valid access ticket
                    giving "read" access rights to the queue's access
                    realm.  Note that purging a queue is equivalent to
                    reading all messages and discarding them.

            queue: shortstr

                Specifies the name of the queue to purge.  If the
                queue name is empty, refers to the current queue for
                the channel, which is the last declared queue.

                RULE:

                    If the client did not previously declare a queue,
                    and the queue name in this method is empty, the
                    server MUST raise a connection exception with
                    reply code 530 (not allowed).

                RULE:

                    The queue must exist. Attempting to purge a non-
                    existing queue causes a channel exception.

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(queue)
        args.write_bit(nowait)
        self.send_method_frame((50, 30), args)
        return self.wait(allowed_methods=[
                          (50, 31),    # Channel.queue_purge_ok
                        ])


    def _queue_purge_ok(self, args):
        """
        confirms a queue purge

        This method confirms the purge of a queue.

        PARAMETERS:
            message_count: long

                number of messages purged

                Reports the number of messages purged.

        """
        message_count = args.read_long()


    #############
    #
    #  Basic
    #
    #
    # work with basic content
    #
    # The Basic class provides methods that support an industry-
    # standard messaging model.
    #
    # GRAMMAR:
    #
    #     basic               = C:QOS S:QOS-OK
    #                         / C:CONSUME S:CONSUME-OK
    #                         / C:CANCEL S:CANCEL-OK
    #                         / C:PUBLISH content
    #                         / S:RETURN content
    #                         / S:DELIVER content
    #                         / C:GET ( S:GET-OK content / S:GET-EMPTY )
    #                         / C:ACK
    #                         / C:REJECT
    #
    # RULE:
    #
    #     The server SHOULD respect the persistent property of basic
    #     messages and SHOULD make a best-effort to hold persistent
    #     basic messages on a reliable storage mechanism.
    #
    # RULE:
    #
    #     The server MUST NOT discard a persistent basic message in
    #     case of a queue overflow. The server MAY use the
    #     Channel.Flow method to slow or stop a basic message
    #     publisher when necessary.
    #
    # RULE:
    #
    #     The server MAY overflow non-persistent basic messages to
    #     persistent storage and MAY discard or dead-letter non-
    #     persistent basic messages on a priority basis if the queue
    #     size exceeds some configured limit.
    #
    # RULE:
    #
    #     The server MUST implement at least 2 priority levels for
    #     basic messages, where priorities 0-4 and 5-9 are treated as
    #     two distinct levels. The server MAY implement up to 10
    #     priority levels.
    #
    # RULE:
    #
    #     The server MUST deliver messages of the same priority in
    #     order irrespective of their individual persistence.
    #
    # RULE:
    #
    #     The server MUST support both automatic and explicit
    #     acknowledgements on Basic content.
    #
    # PARAMETERS:
    #     content_type: shortstr
    #
    #         MIME content type
    #
    #
    #     content_encoding: shortstr
    #
    #         MIME content encoding
    #
    #
    #     headers: table
    #
    #         Message header field table
    #
    #
    #     delivery_mode: octet
    #
    #         Non-persistent (1) or persistent (2)
    #
    #
    #     priority: octet
    #
    #         The message priority, 0 to 9
    #
    #
    #     correlation_id: shortstr
    #
    #         The application correlation identifier
    #
    #
    #     reply_to: shortstr
    #
    #         The destination to reply to
    #
    #
    #     expiration: shortstr
    #
    #         Message expiration specification
    #
    #
    #     message_id: shortstr
    #
    #         The application message identifier
    #
    #
    #     timestamp: timestamp
    #
    #         The message timestamp
    #
    #
    #     type: shortstr
    #
    #         The message type name
    #
    #
    #     user_id: shortstr
    #
    #         The creating user id
    #
    #
    #     app_id: shortstr
    #
    #         The creating application id
    #
    #
    #     cluster_id: shortstr
    #
    #         Intra-cluster routing identifier
    #
    #
    #

    def basic_ack(self, delivery_tag, multiple):
        """
        acknowledge one or more messages

        This method acknowledges one or more messages delivered via
        the Deliver or Get-Ok methods.  The client can ask to confirm
        a single message or a set of messages up to and including a
        specific message.

        PARAMETERS:
            delivery_tag: longlong

            multiple: bit

                acknowledge multiple messages

                If set to 1, the delivery tag is treated as "up to and
                including", so that the client can acknowledge
                multiple messages with a single method.  If set to
                zero, the delivery tag refers to a single message.  If
                the multiple field is 1, and the delivery tag is zero,
                tells the server to acknowledge all outstanding
                mesages.

                RULE:

                    The server MUST validate that a non-zero delivery-
                    tag refers to an delivered message, and raise a
                    channel exception if this is not the case.

        """
        args = AMQPWriter()
        args.write_longlong(delivery_tag)
        args.write_bit(multiple)
        self.send_method_frame((60, 80), args)


    def basic_cancel(self, consumer_tag, nowait):
        """
        end a queue consumer

        This method cancels a consumer. This does not affect already
        delivered messages, but it does mean the server will not send
        any more messages for that consumer.  The client may receive
        an abitrary number of messages in between sending the cancel
        method and receiving the cancel-ok reply.

        RULE:

            If the queue no longer exists when the client sends a
            cancel command, or the consumer has been cancelled for
            other reasons, this command has no effect.

        PARAMETERS:
            consumer_tag: shortstr

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

        """
        args = AMQPWriter()
        args.write_shortstr(consumer_tag)
        args.write_bit(nowait)
        self.send_method_frame((60, 30), args)
        return self.wait(allowed_methods=[
                          (60, 31),    # Channel.basic_cancel_ok
                        ])


    def _basic_cancel_ok(self, args):
        """
        confirm a cancelled consumer

        This method confirms that the cancellation was completed.

        PARAMETERS:
            consumer_tag: shortstr

        """
        consumer_tag = args.read_shortstr()


    def basic_consume(self, queue, consumer_tag, no_local, no_ack, exclusive, nowait, ticket):
        """
        start a queue consumer

        This method asks the server to start a "consumer", which is a
        transient request for messages from a specific queue.
        Consumers last as long as the channel they were created on, or
        until the client cancels them.

        RULE:

            The server SHOULD support at least 16 consumers per queue,
            unless the queue was declared as private, and ideally,
            impose no limit except as defined by available resources.

        PARAMETERS:
            ticket: short

                RULE:

                    The client MUST provide a valid access ticket
                    giving "read" access rights to the realm for the
                    queue.

            queue: shortstr

                Specifies the name of the queue to consume from.  If
                the queue name is null, refers to the current queue
                for the channel, which is the last declared queue.

                RULE:

                    If the client did not previously declare a queue,
                    and the queue name in this method is empty, the
                    server MUST raise a connection exception with
                    reply code 530 (not allowed).

            consumer_tag: shortstr

                Specifies the identifier for the consumer. The
                consumer tag is local to a connection, so two clients
                can use the same consumer tags. If this field is empty
                the server will generate a unique tag.

                RULE:

                    The tag MUST NOT refer to an existing consumer. If
                    the client attempts to create two consumers with
                    the same non-empty tag the server MUST raise a
                    connection exception with reply code 530 (not
                    allowed).

            no_local: bit

            no_ack: bit

            exclusive: bit

                request exclusive access

                Request exclusive consumer access, meaning only this
                consumer can access the queue.

                RULE:

                    If the server cannot grant exclusive access to the
                    queue when asked, - because there are other
                    consumers active - it MUST raise a channel
                    exception with return code 403 (access refused).

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(queue)
        args.write_shortstr(consumer_tag)
        args.write_bit(no_local)
        args.write_bit(no_ack)
        args.write_bit(exclusive)
        args.write_bit(nowait)
        self.send_method_frame((60, 20), args)
        return self.wait(allowed_methods=[
                          (60, 21),    # Channel.basic_consume_ok
                        ])


    def _basic_consume_ok(self, args):
        """
        confirm a new consumer

        The server provides the client with a consumer tag, which is
        used by the client for methods called on the consumer at a
        later stage.

        PARAMETERS:
            consumer_tag: shortstr

                Holds the consumer tag specified by the client or
                provided by the server.

        """
        consumer_tag = args.read_shortstr()


    def _basic_deliver(self, args):
        """
        notify the client of a consumer message

        This method delivers a message to the client, via a consumer.
        In the asynchronous message delivery model, the client starts
        a consumer using the Consume method, then the server responds
        with Deliver methods as and when messages arrive for that
        consumer.

        RULE:

            The server SHOULD track the number of times a message has
            been delivered to clients and when a message is
            redelivered a certain number of times - e.g. 5 times -
            without being acknowledged, the server SHOULD consider the
            message to be unprocessable (possibly causing client
            applications to abort), and move the message to a dead
            letter queue.

        PARAMETERS:
            consumer_tag: shortstr

            delivery_tag: longlong

            redelivered: bit

            exchange: shortstr

                Specifies the name of the exchange that the message
                was originally published to.

            routing_key: shortstr

                Message routing key

                Specifies the routing key name specified when the
                message was published.

        """
        consumer_tag = args.read_shortstr()
        delivery_tag = args.read_longlong()
        redelivered = args.read_bit()
        exchange = args.read_shortstr()
        routing_key = args.read_shortstr()
        msg = self.wait()


    def basic_get(self, queue, no_ack, ticket):
        """
        direct access to a queue

        This method provides a direct access to the messages in a
        queue using a synchronous dialogue that is designed for
        specific types of application where synchronous functionality
        is more important than performance.

        PARAMETERS:
            ticket: short

                RULE:

                    The client MUST provide a valid access ticket
                    giving "read" access rights to the realm for the
                    queue.

            queue: shortstr

                Specifies the name of the queue to consume from.  If
                the queue name is null, refers to the current queue
                for the channel, which is the last declared queue.

                RULE:

                    If the client did not previously declare a queue,
                    and the queue name in this method is empty, the
                    server MUST raise a connection exception with
                    reply code 530 (not allowed).

            no_ack: bit

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(queue)
        args.write_bit(no_ack)
        self.send_method_frame((60, 70), args)
        return self.wait(allowed_methods=[
                          (60, 71),    # Channel.basic_get_ok
                          (60, 72),    # Channel.basic_get_empty
                        ])


    def _basic_get_empty(self, args):
        """
        indicate no messages available

        This method tells the client that the queue has no messages
        available for the client.

        PARAMETERS:
            cluster_id: shortstr

                Cluster id

                For use by cluster applications, should not be used by
                client applications.

        """
        cluster_id = args.read_shortstr()


    def _basic_get_ok(self, args):
        """
        provide client with a message

        This method delivers a message to the client following a get
        method.  A message delivered by 'get-ok' must be acknowledged
        unless the no-ack option was set in the get method.

        PARAMETERS:
            delivery_tag: longlong

            redelivered: bit

            exchange: shortstr

                Specifies the name of the exchange that the message
                was originally published to.  If empty, the message
                was published to the default exchange.

            routing_key: shortstr

                Message routing key

                Specifies the routing key name specified when the
                message was published.

            message_count: long

                number of messages pending

                This field reports the number of messages pending on
                the queue, excluding the message being delivered.
                Note that this figure is indicative, not reliable, and
                can change arbitrarily as messages are added to the
                queue and removed by other clients.

        """
        delivery_tag = args.read_longlong()
        redelivered = args.read_bit()
        exchange = args.read_shortstr()
        routing_key = args.read_shortstr()
        message_count = args.read_long()
        msg = self.wait()


    def basic_publish(self, msg, exchange, routing_key, mandatory, immediate, ticket):
        """
        publish a message

        This method publishes a message to a specific exchange. The
        message will be routed to queues as defined by the exchange
        configuration and distributed to any active consumers when the
        transaction, if any, is committed.

        PARAMETERS:
            ticket: short

                RULE:

                    The client MUST provide a valid access ticket
                    giving "write" access rights to the access realm
                    for the exchange.

            exchange: shortstr

                Specifies the name of the exchange to publish to.  The
                exchange name can be empty, meaning the default
                exchange.  If the exchange name is specified, and that
                exchange does not exist, the server will raise a
                channel exception.

                RULE:

                    The server MUST accept a blank exchange name to
                    mean the default exchange.

                RULE:

                    If the exchange was declared as an internal
                    exchange, the server MUST raise a channel
                    exception with a reply code 403 (access refused).

                RULE:

                    The exchange MAY refuse basic content in which
                    case it MUST raise a channel exception with reply
                    code 540 (not implemented).

            routing_key: shortstr

                Message routing key

                Specifies the routing key for the message.  The
                routing key is used for routing messages depending on
                the exchange configuration.

            mandatory: bit

                indicate mandatory routing

                This flag tells the server how to react if the message
                cannot be routed to a queue.  If this flag is set, the
                server will return an unroutable message with a Return
                method.  If this flag is zero, the server silently
                drops the message.

                RULE:

                    The server SHOULD implement the mandatory flag.

            immediate: bit

                request immediate delivery

                This flag tells the server how to react if the message
                cannot be routed to a queue consumer immediately.  If
                this flag is set, the server will return an
                undeliverable message with a Return method. If this
                flag is zero, the server will queue the message, but
                with no guarantee that it will ever be consumed.

                RULE:

                    The server SHOULD implement the immediate flag.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(exchange)
        args.write_shortstr(routing_key)
        args.write_bit(mandatory)
        args.write_bit(immediate)
        self.send_method_frame((60, 40), args)


    def basic_qos(self, prefetch_size, prefetch_count, a_global):
        """
        specify quality of service

        This method requests a specific quality of service.  The QoS
        can be specified for the current channel or for all channels
        on the connection.  The particular properties and semantics of
        a qos method always depend on the content class semantics.
        Though the qos method could in principle apply to both peers,
        it is currently meaningful only for the server.

        PARAMETERS:
            prefetch_size: long

                prefetch window in octets

                The client can request that messages be sent in
                advance so that when the client finishes processing a
                message, the following message is already held
                locally, rather than needing to be sent down the
                channel.  Prefetching gives a performance improvement.
                This field specifies the prefetch window size in
                octets.  The server will send a message in advance if
                it is equal to or smaller in size than the available
                prefetch size (and also falls into other prefetch
                limits). May be set to zero, meaning "no specific
                limit", although other prefetch limits may still
                apply. The prefetch-size is ignored if the no-ack
                option is set.

                RULE:

                    The server MUST ignore this setting when the
                    client is not processing any messages - i.e. the
                    prefetch size does not limit the transfer of
                    single messages to a client, only the sending in
                    advance of more messages while the client still
                    has one or more unacknowledged messages.

            prefetch_count: short

                prefetch window in messages

                Specifies a prefetch window in terms of whole
                messages.  This field may be used in combination with
                the prefetch-size field; a message will only be sent
                in advance if both prefetch windows (and those at the
                channel and connection level) allow it. The prefetch-
                count is ignored if the no-ack option is set.

                RULE:

                    The server MAY send less data in advance than
                    allowed by the client's specified prefetch windows
                    but it MUST NOT send more.

            a_global: bit

                apply to entire connection

                By default the QoS settings apply to the current
                channel only.  If this field is set, they are applied
                to the entire connection.

        """
        args = AMQPWriter()
        args.write_long(prefetch_size)
        args.write_short(prefetch_count)
        args.write_bit(a_global)
        self.send_method_frame((60, 10), args)
        return self.wait(allowed_methods=[
                          (60, 11),    # Channel.basic_qos_ok
                        ])


    def _basic_qos_ok(self, args):
        """
        confirm the requested qos

        This method tells the client that the requested QoS levels
        could be handled by the server.  The requested QoS applies to
        all active consumers until a new QoS is defined.

        """
        pass


    def basic_recover(self, requeue):
        """
        redeliver unacknowledged messages. This method is only allowed
        on non-transacted channels.

        This method asks the broker to redeliver all unacknowledged
        messages on a specifieid channel. Zero or more messages may be
        redelivered.

        RULE:

            The server MUST set the redelivered flag on all messages
            that are resent.

        RULE:

            The server MUST raise a channel exception if this is
            called on a transacted channel.

        PARAMETERS:
            requeue: bit

                requeue the message

                If this field is zero, the message will be redelivered
                to the original recipient.  If this bit is 1, the
                server will attempt to requeue the message,
                potentially then delivering it to an alternative
                subscriber.

        """
        args = AMQPWriter()
        args.write_bit(requeue)
        self.send_method_frame((60, 100), args)


    def basic_reject(self, delivery_tag, requeue):
        """
        reject an incoming message

        This method allows a client to reject a message.  It can be
        used to interrupt and cancel large incoming messages, or
        return untreatable messages to their original queue.

        RULE:

            The server SHOULD be capable of accepting and process the
            Reject method while sending message content with a Deliver
            or Get-Ok method.  I.e. the server should read and process
            incoming methods while sending output frames.  To cancel a
            partially-send content, the server sends a content body
            frame of size 1 (i.e. with no data except the frame-end
            octet).

        RULE:

            The server SHOULD interpret this method as meaning that
            the client is unable to process the message at this time.

        RULE:

            A client MUST NOT use this method as a means of selecting
            messages to process.  A rejected message MAY be discarded
            or dead-lettered, not necessarily passed to another
            client.

        PARAMETERS:
            delivery_tag: longlong

            requeue: bit

                requeue the message

                If this field is zero, the message will be discarded.
                If this bit is 1, the server will attempt to requeue
                the message.

                RULE:

                    The server MUST NOT deliver the message to the
                    same client within the context of the current
                    channel.  The recommended strategy is to attempt
                    to deliver the message to an alternative consumer,
                    and if that is not possible, to move the message
                    to a dead-letter queue.  The server MAY use more
                    sophisticated tracking to hold the message on the
                    queue and redeliver it to the same client at a
                    later stage.

        """
        args = AMQPWriter()
        args.write_longlong(delivery_tag)
        args.write_bit(requeue)
        self.send_method_frame((60, 90), args)


    def _basic_return(self, args):
        """
        return a failed message

        This method returns an undeliverable message that was
        published with the "immediate" flag set, or an unroutable
        message published with the "mandatory" flag set. The reply
        code and text provide information about the reason that the
        message was undeliverable.

        PARAMETERS:
            reply_code: short

            reply_text: shortstr

            exchange: shortstr

                Specifies the name of the exchange that the message
                was originally published to.

            routing_key: shortstr

                Message routing key

                Specifies the routing key name specified when the
                message was published.

        """
        reply_code = args.read_short()
        reply_text = args.read_shortstr()
        exchange = args.read_shortstr()
        routing_key = args.read_shortstr()
        msg = self.wait()


    #############
    #
    #  File
    #
    #
    # work with file content
    #
    # The file class provides methods that support reliable file
    # transfer. File messages have a specific set of properties that
    # are required for interoperability with file transfer
    # applications. File messages and acknowledgements are subject to
    # channel transactions.  Note that the file class does not provide
    # message browsing methods; these are not compatible with the
    # staging model.  Applications that need browsable file transfer
    # should use Basic content and the Basic class.
    #
    # GRAMMAR:
    #
    #     file                = C:QOS S:QOS-OK
    #                         / C:CONSUME S:CONSUME-OK
    #                         / C:CANCEL S:CANCEL-OK
    #                         / C:OPEN S:OPEN-OK C:STAGE content
    #                         / S:OPEN C:OPEN-OK S:STAGE content
    #                         / C:PUBLISH
    #                         / S:DELIVER
    #                         / S:RETURN
    #                         / C:ACK
    #                         / C:REJECT
    #
    # RULE:
    #
    #     The server MUST make a best-effort to hold file messages on
    #     a reliable storage mechanism.
    #
    # RULE:
    #
    #     The server MUST NOT discard a file message in case of a
    #     queue overflow. The server MUST use the Channel.Flow method
    #     to slow or stop a file message publisher when necessary.
    #
    # RULE:
    #
    #     The server MUST implement at least 2 priority levels for
    #     file messages, where priorities 0-4 and 5-9 are treated as
    #     two distinct levels. The server MAY implement up to 10
    #     priority levels.
    #
    # RULE:
    #
    #     The server MUST support both automatic and explicit
    #     acknowledgements on file content.
    #
    # PARAMETERS:
    #     content_type: shortstr
    #
    #         MIME content type
    #
    #
    #     content_encoding: shortstr
    #
    #         MIME content encoding
    #
    #
    #     headers: table
    #
    #         Message header field table
    #
    #
    #     priority: octet
    #
    #         The message priority, 0 to 9
    #
    #
    #     reply_to: shortstr
    #
    #         The destination to reply to
    #
    #
    #     message_id: shortstr
    #
    #         The application message identifier
    #
    #
    #     filename: shortstr
    #
    #         The message filename
    #
    #
    #     timestamp: timestamp
    #
    #         The message timestamp
    #
    #
    #     cluster_id: shortstr
    #
    #         Intra-cluster routing identifier
    #
    #
    #

    def file_ack(self, delivery_tag, multiple):
        """
        acknowledge one or more messages

        This method acknowledges one or more messages delivered via
        the Deliver method.  The client can ask to confirm a single
        message or a set of messages up to and including a specific
        message.

        PARAMETERS:
            delivery_tag: longlong

            multiple: bit

                acknowledge multiple messages

                If set to 1, the delivery tag is treated as "up to and
                including", so that the client can acknowledge
                multiple messages with a single method.  If set to
                zero, the delivery tag refers to a single message.  If
                the multiple field is 1, and the delivery tag is zero,
                tells the server to acknowledge all outstanding
                mesages.

                RULE:

                    The server MUST validate that a non-zero delivery-
                    tag refers to an delivered message, and raise a
                    channel exception if this is not the case.

        """
        args = AMQPWriter()
        args.write_longlong(delivery_tag)
        args.write_bit(multiple)
        self.send_method_frame((70, 90), args)


    def file_cancel(self, consumer_tag, nowait):
        """
        end a queue consumer

        This method cancels a consumer. This does not affect already
        delivered messages, but it does mean the server will not send
        any more messages for that consumer.

        PARAMETERS:
            consumer_tag: shortstr

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

        """
        args = AMQPWriter()
        args.write_shortstr(consumer_tag)
        args.write_bit(nowait)
        self.send_method_frame((70, 30), args)
        return self.wait(allowed_methods=[
                          (70, 31),    # Channel.file_cancel_ok
                        ])


    def _file_cancel_ok(self, args):
        """
        confirm a cancelled consumer

        This method confirms that the cancellation was completed.

        PARAMETERS:
            consumer_tag: shortstr

        """
        consumer_tag = args.read_shortstr()


    def file_consume(self, queue, consumer_tag, no_local, no_ack, exclusive, nowait, ticket):
        """
        start a queue consumer

        This method asks the server to start a "consumer", which is a
        transient request for messages from a specific queue.
        Consumers last as long as the channel they were created on, or
        until the client cancels them.

        RULE:

            The server SHOULD support at least 16 consumers per queue,
            unless the queue was declared as private, and ideally,
            impose no limit except as defined by available resources.

        PARAMETERS:
            ticket: short

                RULE:

                    The client MUST provide a valid access ticket
                    giving "read" access rights to the realm for the
                    queue.

            queue: shortstr

                Specifies the name of the queue to consume from.  If
                the queue name is null, refers to the current queue
                for the channel, which is the last declared queue.

                RULE:

                    If the client did not previously declare a queue,
                    and the queue name in this method is empty, the
                    server MUST raise a connection exception with
                    reply code 530 (not allowed).

            consumer_tag: shortstr

                Specifies the identifier for the consumer. The
                consumer tag is local to a connection, so two clients
                can use the same consumer tags. If this field is empty
                the server will generate a unique tag.

                RULE:

                    The tag MUST NOT refer to an existing consumer. If
                    the client attempts to create two consumers with
                    the same non-empty tag the server MUST raise a
                    connection exception with reply code 530 (not
                    allowed).

            no_local: bit

            no_ack: bit

            exclusive: bit

                request exclusive access

                Request exclusive consumer access, meaning only this
                consumer can access the queue.

                RULE:

                    If the server cannot grant exclusive access to the
                    queue when asked, - because there are other
                    consumers active - it MUST raise a channel
                    exception with return code 405 (resource locked).

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(queue)
        args.write_shortstr(consumer_tag)
        args.write_bit(no_local)
        args.write_bit(no_ack)
        args.write_bit(exclusive)
        args.write_bit(nowait)
        self.send_method_frame((70, 20), args)
        return self.wait(allowed_methods=[
                          (70, 21),    # Channel.file_consume_ok
                        ])


    def _file_consume_ok(self, args):
        """
        confirm a new consumer

        This method provides the client with a consumer tag which it
        MUST use in methods that work with the consumer.

        PARAMETERS:
            consumer_tag: shortstr

                Holds the consumer tag specified by the client or
                provided by the server.

        """
        consumer_tag = args.read_shortstr()


    def _file_deliver(self, args):
        """
        notify the client of a consumer message

        This method delivers a staged file message to the client, via
        a consumer. In the asynchronous message delivery model, the
        client starts a consumer using the Consume method, then the
        server responds with Deliver methods as and when messages
        arrive for that consumer.

        RULE:

            The server SHOULD track the number of times a message has
            been delivered to clients and when a message is
            redelivered a certain number of times - e.g. 5 times -
            without being acknowledged, the server SHOULD consider the
            message to be unprocessable (possibly causing client
            applications to abort), and move the message to a dead
            letter queue.

        PARAMETERS:
            consumer_tag: shortstr

            delivery_tag: longlong

            redelivered: bit

            exchange: shortstr

                Specifies the name of the exchange that the message
                was originally published to.

            routing_key: shortstr

                Message routing key

                Specifies the routing key name specified when the
                message was published.

            identifier: shortstr

                staging identifier

                This is the staging identifier of the message to
                deliver.  The message must have been staged.  Note
                that a server can send the Deliver method
                asynchronously without waiting for staging to finish.

        """
        consumer_tag = args.read_shortstr()
        delivery_tag = args.read_longlong()
        redelivered = args.read_bit()
        exchange = args.read_shortstr()
        routing_key = args.read_shortstr()
        identifier = args.read_shortstr()


    def file_open(self, identifier, content_size):
        """
        request to start staging

        This method requests permission to start staging a message.
        Staging means sending the message into a temporary area at the
        recipient end and then delivering the message by referring to
        this temporary area. Staging is how the protocol handles
        partial file transfers - if a message is partially staged and
        the connection breaks, the next time the sender starts to
        stage it, it can restart from where it left off.

        PARAMETERS:
            identifier: shortstr

                staging identifier

                This is the staging identifier. This is an arbitrary
                string chosen by the sender.  For staging to work
                correctly the sender must use the same staging
                identifier when staging the same message a second time
                after recovery from a failure.  A good choice for the
                staging identifier would be the SHA1 hash of the
                message properties data (including the original
                filename, revised time, etc.).

            content_size: longlong

                message content size

                The size of the content in octets.  The recipient may
                use this information to allocate or check available
                space in advance, to avoid "disk full" errors during
                staging of very large messages.

                RULE:

                    The sender MUST accurately fill the content-size
                    field. Zero-length content is permitted.

        """
        args = AMQPWriter()
        args.write_shortstr(identifier)
        args.write_longlong(content_size)
        self.send_method_frame((70, 40), args)
        return self.wait(allowed_methods=[
                          (70, 41),    # Channel.file_open_ok
                        ])


    def _file_open(self, args):
        """
        request to start staging

        This method requests permission to start staging a message.
        Staging means sending the message into a temporary area at the
        recipient end and then delivering the message by referring to
        this temporary area. Staging is how the protocol handles
        partial file transfers - if a message is partially staged and
        the connection breaks, the next time the sender starts to
        stage it, it can restart from where it left off.

        PARAMETERS:
            identifier: shortstr

                staging identifier

                This is the staging identifier. This is an arbitrary
                string chosen by the sender.  For staging to work
                correctly the sender must use the same staging
                identifier when staging the same message a second time
                after recovery from a failure.  A good choice for the
                staging identifier would be the SHA1 hash of the
                message properties data (including the original
                filename, revised time, etc.).

            content_size: longlong

                message content size

                The size of the content in octets.  The recipient may
                use this information to allocate or check available
                space in advance, to avoid "disk full" errors during
                staging of very large messages.

                RULE:

                    The sender MUST accurately fill the content-size
                    field. Zero-length content is permitted.

        """
        identifier = args.read_shortstr()
        content_size = args.read_longlong()


    def file_open_ok(self, staged_size):
        """
        confirm staging ready

        This method confirms that the recipient is ready to accept
        staged data.  If the message was already partially-staged at a
        previous time the recipient will report the number of octets
        already staged.

        PARAMETERS:
            staged_size: longlong

                already staged amount

                The amount of previously-staged content in octets.
                For a new message this will be zero.

                RULE:

                    The sender MUST start sending data from this octet
                    offset in the message, counting from zero.

                RULE:

                    The recipient MAY decide how long to hold
                    partially-staged content and MAY implement staging
                    by always discarding partially-staged content.
                    However if it uses the file content type it MUST
                    support the staging methods.

        """
        args = AMQPWriter()
        args.write_longlong(staged_size)
        self.send_method_frame((70, 41), args)
        return self.wait(allowed_methods=[
                          (70, 50),    # Channel.file_stage
                        ])


    def _file_open_ok(self, args):
        """
        confirm staging ready

        This method confirms that the recipient is ready to accept
        staged data.  If the message was already partially-staged at a
        previous time the recipient will report the number of octets
        already staged.

        PARAMETERS:
            staged_size: longlong

                already staged amount

                The amount of previously-staged content in octets.
                For a new message this will be zero.

                RULE:

                    The sender MUST start sending data from this octet
                    offset in the message, counting from zero.

                RULE:

                    The recipient MAY decide how long to hold
                    partially-staged content and MAY implement staging
                    by always discarding partially-staged content.
                    However if it uses the file content type it MUST
                    support the staging methods.

        """
        staged_size = args.read_longlong()


    def file_publish(self, exchange, routing_key, mandatory, immediate, identifier, ticket):
        """
        publish a message

        This method publishes a staged file message to a specific
        exchange. The file message will be routed to queues as defined
        by the exchange configuration and distributed to any active
        consumers when the transaction, if any, is committed.

        PARAMETERS:
            ticket: short

                RULE:

                    The client MUST provide a valid access ticket
                    giving "write" access rights to the access realm
                    for the exchange.

            exchange: shortstr

                Specifies the name of the exchange to publish to.  The
                exchange name can be empty, meaning the default
                exchange.  If the exchange name is specified, and that
                exchange does not exist, the server will raise a
                channel exception.

                RULE:

                    The server MUST accept a blank exchange name to
                    mean the default exchange.

                RULE:

                    If the exchange was declared as an internal
                    exchange, the server MUST respond with a reply
                    code 403 (access refused) and raise a channel
                    exception.

                RULE:

                    The exchange MAY refuse file content in which case
                    it MUST respond with a reply code 540 (not
                    implemented) and raise a channel exception.

            routing_key: shortstr

                Message routing key

                Specifies the routing key for the message.  The
                routing key is used for routing messages depending on
                the exchange configuration.

            mandatory: bit

                indicate mandatory routing

                This flag tells the server how to react if the message
                cannot be routed to a queue.  If this flag is set, the
                server will return an unroutable message with a Return
                method.  If this flag is zero, the server silently
                drops the message.

                RULE:

                    The server SHOULD implement the mandatory flag.

            immediate: bit

                request immediate delivery

                This flag tells the server how to react if the message
                cannot be routed to a queue consumer immediately.  If
                this flag is set, the server will return an
                undeliverable message with a Return method. If this
                flag is zero, the server will queue the message, but
                with no guarantee that it will ever be consumed.

                RULE:

                    The server SHOULD implement the immediate flag.

            identifier: shortstr

                staging identifier

                This is the staging identifier of the message to
                publish.  The message must have been staged.  Note
                that a client can send the Publish method
                asynchronously without waiting for staging to finish.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(exchange)
        args.write_shortstr(routing_key)
        args.write_bit(mandatory)
        args.write_bit(immediate)
        args.write_shortstr(identifier)
        self.send_method_frame((70, 60), args)


    def file_qos(self, prefetch_size, prefetch_count, a_global):
        """
        specify quality of service

        This method requests a specific quality of service.  The QoS
        can be specified for the current channel or for all channels
        on the connection.  The particular properties and semantics of
        a qos method always depend on the content class semantics.
        Though the qos method could in principle apply to both peers,
        it is currently meaningful only for the server.

        PARAMETERS:
            prefetch_size: long

                prefetch window in octets

                The client can request that messages be sent in
                advance so that when the client finishes processing a
                message, the following message is already held
                locally, rather than needing to be sent down the
                channel.  Prefetching gives a performance improvement.
                This field specifies the prefetch window size in
                octets. May be set to zero, meaning "no specific
                limit".  Note that other prefetch limits may still
                apply. The prefetch-size is ignored if the no-ack
                option is set.

            prefetch_count: short

                prefetch window in messages

                Specifies a prefetch window in terms of whole
                messages.  This is compatible with some file API
                implementations.  This field may be used in
                combination with the prefetch-size field; a message
                will only be sent in advance if both prefetch windows
                (and those at the channel and connection level) allow
                it. The prefetch-count is ignored if the no-ack option
                is set.

                RULE:

                    The server MAY send less data in advance than
                    allowed by the client's specified prefetch windows
                    but it MUST NOT send more.

            a_global: bit

                apply to entire connection

                By default the QoS settings apply to the current
                channel only.  If this field is set, they are applied
                to the entire connection.

        """
        args = AMQPWriter()
        args.write_long(prefetch_size)
        args.write_short(prefetch_count)
        args.write_bit(a_global)
        self.send_method_frame((70, 10), args)
        return self.wait(allowed_methods=[
                          (70, 11),    # Channel.file_qos_ok
                        ])


    def _file_qos_ok(self, args):
        """
        confirm the requested qos

        This method tells the client that the requested QoS levels
        could be handled by the server.  The requested QoS applies to
        all active consumers until a new QoS is defined.

        """
        pass


    def file_reject(self, delivery_tag, requeue):
        """
        reject an incoming message

        This method allows a client to reject a message.  It can be
        used to return untreatable messages to their original queue.
        Note that file content is staged before delivery, so the
        client will not use this method to interrupt delivery of a
        large message.

        RULE:

            The server SHOULD interpret this method as meaning that
            the client is unable to process the message at this time.

        RULE:

            A client MUST NOT use this method as a means of selecting
            messages to process.  A rejected message MAY be discarded
            or dead-lettered, not necessarily passed to another
            client.

        PARAMETERS:
            delivery_tag: longlong

            requeue: bit

                requeue the message

                If this field is zero, the message will be discarded.
                If this bit is 1, the server will attempt to requeue
                the message.

                RULE:

                    The server MUST NOT deliver the message to the
                    same client within the context of the current
                    channel.  The recommended strategy is to attempt
                    to deliver the message to an alternative consumer,
                    and if that is not possible, to move the message
                    to a dead-letter queue.  The server MAY use more
                    sophisticated tracking to hold the message on the
                    queue and redeliver it to the same client at a
                    later stage.

        """
        args = AMQPWriter()
        args.write_longlong(delivery_tag)
        args.write_bit(requeue)
        self.send_method_frame((70, 100), args)


    def _file_return(self, args):
        """
        return a failed message

        This method returns an undeliverable message that was
        published with the "immediate" flag set, or an unroutable
        message published with the "mandatory" flag set. The reply
        code and text provide information about the reason that the
        message was undeliverable.

        PARAMETERS:
            reply_code: short

            reply_text: shortstr

            exchange: shortstr

                Specifies the name of the exchange that the message
                was originally published to.

            routing_key: shortstr

                Message routing key

                Specifies the routing key name specified when the
                message was published.

        """
        reply_code = args.read_short()
        reply_text = args.read_shortstr()
        exchange = args.read_shortstr()
        routing_key = args.read_shortstr()
        msg = self.wait()


    def file_stage(self, msg):
        """
        stage message content

        This method stages the message, sending the message content to
        the recipient from the octet offset specified in the Open-Ok
        method.

        """
        self.send_method_frame((70, 50))


    def _file_stage(self, args):
        """
        stage message content

        This method stages the message, sending the message content to
        the recipient from the octet offset specified in the Open-Ok
        method.

        """
        msg = self.wait()


    #############
    #
    #  Stream
    #
    #
    # work with streaming content
    #
    # The stream class provides methods that support multimedia
    # streaming. The stream class uses the following semantics: one
    # message is one packet of data; delivery is unacknowleged and
    # unreliable; the consumer can specify quality of service
    # parameters that the server can try to adhere to; lower-priority
    # messages may be discarded in favour of high priority messages.
    #
    # GRAMMAR:
    #
    #     stream              = C:QOS S:QOS-OK
    #                         / C:CONSUME S:CONSUME-OK
    #                         / C:CANCEL S:CANCEL-OK
    #                         / C:PUBLISH content
    #                         / S:RETURN
    #                         / S:DELIVER content
    #
    # RULE:
    #
    #     The server SHOULD discard stream messages on a priority
    #     basis if the queue size exceeds some configured limit.
    #
    # RULE:
    #
    #     The server MUST implement at least 2 priority levels for
    #     stream messages, where priorities 0-4 and 5-9 are treated as
    #     two distinct levels. The server MAY implement up to 10
    #     priority levels.
    #
    # RULE:
    #
    #     The server MUST implement automatic acknowledgements on
    #     stream content.  That is, as soon as a message is delivered
    #     to a client via a Deliver method, the server must remove it
    #     from the queue.
    #
    # PARAMETERS:
    #     content_type: shortstr
    #
    #         MIME content type
    #
    #
    #     content_encoding: shortstr
    #
    #         MIME content encoding
    #
    #
    #     headers: table
    #
    #         Message header field table
    #
    #
    #     priority: octet
    #
    #         The message priority, 0 to 9
    #
    #
    #     timestamp: timestamp
    #
    #         The message timestamp
    #
    #
    #

    def stream_cancel(self, consumer_tag, nowait):
        """
        end a queue consumer

        This method cancels a consumer.  Since message delivery is
        asynchronous the client may continue to receive messages for a
        short while after canceling a consumer.  It may process or
        discard these as appropriate.

        PARAMETERS:
            consumer_tag: shortstr

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

        """
        args = AMQPWriter()
        args.write_shortstr(consumer_tag)
        args.write_bit(nowait)
        self.send_method_frame((80, 30), args)
        return self.wait(allowed_methods=[
                          (80, 31),    # Channel.stream_cancel_ok
                        ])


    def _stream_cancel_ok(self, args):
        """
        confirm a cancelled consumer

        This method confirms that the cancellation was completed.

        PARAMETERS:
            consumer_tag: shortstr

        """
        consumer_tag = args.read_shortstr()


    def stream_consume(self, queue, consumer_tag, no_local, exclusive, nowait, ticket):
        """
        start a queue consumer

        This method asks the server to start a "consumer", which is a
        transient request for messages from a specific queue.
        Consumers last as long as the channel they were created on, or
        until the client cancels them.

        RULE:

            The server SHOULD support at least 16 consumers per queue,
            unless the queue was declared as private, and ideally,
            impose no limit except as defined by available resources.

        RULE:

            Streaming applications SHOULD use different channels to
            select different streaming resolutions. AMQP makes no
            provision for filtering and/or transforming streams except
            on the basis of priority-based selective delivery of
            individual messages.

        PARAMETERS:
            ticket: short

                RULE:

                    The client MUST provide a valid access ticket
                    giving "read" access rights to the realm for the
                    queue.

            queue: shortstr

                Specifies the name of the queue to consume from.  If
                the queue name is null, refers to the current queue
                for the channel, which is the last declared queue.

                RULE:

                    If the client did not previously declare a queue,
                    and the queue name in this method is empty, the
                    server MUST raise a connection exception with
                    reply code 530 (not allowed).

            consumer_tag: shortstr

                Specifies the identifier for the consumer. The
                consumer tag is local to a connection, so two clients
                can use the same consumer tags. If this field is empty
                the server will generate a unique tag.

                RULE:

                    The tag MUST NOT refer to an existing consumer. If
                    the client attempts to create two consumers with
                    the same non-empty tag the server MUST raise a
                    connection exception with reply code 530 (not
                    allowed).

            no_local: bit

            exclusive: bit

                request exclusive access

                Request exclusive consumer access, meaning only this
                consumer can access the queue.

                RULE:

                    If the server cannot grant exclusive access to the
                    queue when asked, - because there are other
                    consumers active - it MUST raise a channel
                    exception with return code 405 (resource locked).

            nowait: bit

                do not send a reply method

                If set, the server will not respond to the method. The
                client should not wait for a reply method.  If the
                server could not complete the method it will raise a
                channel or connection exception.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(queue)
        args.write_shortstr(consumer_tag)
        args.write_bit(no_local)
        args.write_bit(exclusive)
        args.write_bit(nowait)
        self.send_method_frame((80, 20), args)
        return self.wait(allowed_methods=[
                          (80, 21),    # Channel.stream_consume_ok
                        ])


    def _stream_consume_ok(self, args):
        """
        confirm a new consumer

        This method provides the client with a consumer tag which it
        may use in methods that work with the consumer.

        PARAMETERS:
            consumer_tag: shortstr

                Holds the consumer tag specified by the client or
                provided by the server.

        """
        consumer_tag = args.read_shortstr()


    def _stream_deliver(self, args):
        """
        notify the client of a consumer message

        This method delivers a message to the client, via a consumer.
        In the asynchronous message delivery model, the client starts
        a consumer using the Consume method, then the server responds
        with Deliver methods as and when messages arrive for that
        consumer.

        PARAMETERS:
            consumer_tag: shortstr

            delivery_tag: longlong

            exchange: shortstr

                Specifies the name of the exchange that the message
                was originally published to.

            queue: shortstr

                Specifies the name of the queue that the message came
                from. Note that a single channel can start many
                consumers on different queues.

        """
        consumer_tag = args.read_shortstr()
        delivery_tag = args.read_longlong()
        exchange = args.read_shortstr()
        queue = args.read_shortstr()
        msg = self.wait()


    def stream_publish(self, msg, exchange, routing_key, mandatory, immediate, ticket):
        """
        publish a message

        This method publishes a message to a specific exchange. The
        message will be routed to queues as defined by the exchange
        configuration and distributed to any active consumers as
        appropriate.

        PARAMETERS:
            ticket: short

                RULE:

                    The client MUST provide a valid access ticket
                    giving "write" access rights to the access realm
                    for the exchange.

            exchange: shortstr

                Specifies the name of the exchange to publish to.  The
                exchange name can be empty, meaning the default
                exchange.  If the exchange name is specified, and that
                exchange does not exist, the server will raise a
                channel exception.

                RULE:

                    The server MUST accept a blank exchange name to
                    mean the default exchange.

                RULE:

                    If the exchange was declared as an internal
                    exchange, the server MUST respond with a reply
                    code 403 (access refused) and raise a channel
                    exception.

                RULE:

                    The exchange MAY refuse stream content in which
                    case it MUST respond with a reply code 540 (not
                    implemented) and raise a channel exception.

            routing_key: shortstr

                Message routing key

                Specifies the routing key for the message.  The
                routing key is used for routing messages depending on
                the exchange configuration.

            mandatory: bit

                indicate mandatory routing

                This flag tells the server how to react if the message
                cannot be routed to a queue.  If this flag is set, the
                server will return an unroutable message with a Return
                method.  If this flag is zero, the server silently
                drops the message.

                RULE:

                    The server SHOULD implement the mandatory flag.

            immediate: bit

                request immediate delivery

                This flag tells the server how to react if the message
                cannot be routed to a queue consumer immediately.  If
                this flag is set, the server will return an
                undeliverable message with a Return method. If this
                flag is zero, the server will queue the message, but
                with no guarantee that it will ever be consumed.

                RULE:

                    The server SHOULD implement the immediate flag.

        """
        args = AMQPWriter()
        args.write_short(ticket)
        args.write_shortstr(exchange)
        args.write_shortstr(routing_key)
        args.write_bit(mandatory)
        args.write_bit(immediate)
        self.send_method_frame((80, 40), args)


    def stream_qos(self, prefetch_size, prefetch_count, consume_rate, a_global):
        """
        specify quality of service

        This method requests a specific quality of service.  The QoS
        can be specified for the current channel or for all channels
        on the connection.  The particular properties and semantics of
        a qos method always depend on the content class semantics.
        Though the qos method could in principle apply to both peers,
        it is currently meaningful only for the server.

        PARAMETERS:
            prefetch_size: long

                prefetch window in octets

                The client can request that messages be sent in
                advance so that when the client finishes processing a
                message, the following message is already held
                locally, rather than needing to be sent down the
                channel.  Prefetching gives a performance improvement.
                This field specifies the prefetch window size in
                octets. May be set to zero, meaning "no specific
                limit".  Note that other prefetch limits may still
                apply.

            prefetch_count: short

                prefetch window in messages

                Specifies a prefetch window in terms of whole
                messages.  This field may be used in combination with
                the prefetch-size field; a message will only be sent
                in advance if both prefetch windows (and those at the
                channel and connection level) allow it.

            consume_rate: long

                transfer rate in octets/second

                Specifies a desired transfer rate in octets per
                second. This is usually determined by the application
                that uses the streaming data.  A value of zero means
                "no limit", i.e. as rapidly as possible.

                RULE:

                    The server MAY ignore the prefetch values and
                    consume rates, depending on the type of stream and
                    the ability of the server to queue and/or reply
                    it.  The server MAY drop low-priority messages in
                    favour of high-priority messages.

            a_global: bit

                apply to entire connection

                By default the QoS settings apply to the current
                channel only.  If this field is set, they are applied
                to the entire connection.

        """
        args = AMQPWriter()
        args.write_long(prefetch_size)
        args.write_short(prefetch_count)
        args.write_long(consume_rate)
        args.write_bit(a_global)
        self.send_method_frame((80, 10), args)
        return self.wait(allowed_methods=[
                          (80, 11),    # Channel.stream_qos_ok
                        ])


    def _stream_qos_ok(self, args):
        """
        confirm the requested qos

        This method tells the client that the requested QoS levels
        could be handled by the server.  The requested QoS applies to
        all active consumers until a new QoS is defined.

        """
        pass


    def _stream_return(self, args):
        """
        return a failed message

        This method returns an undeliverable message that was
        published with the "immediate" flag set, or an unroutable
        message published with the "mandatory" flag set. The reply
        code and text provide information about the reason that the
        message was undeliverable.

        PARAMETERS:
            reply_code: short

            reply_text: shortstr

            exchange: shortstr

                Specifies the name of the exchange that the message
                was originally published to.

            routing_key: shortstr

                Message routing key

                Specifies the routing key name specified when the
                message was published.

        """
        reply_code = args.read_short()
        reply_text = args.read_shortstr()
        exchange = args.read_shortstr()
        routing_key = args.read_shortstr()
        msg = self.wait()


    #############
    #
    #  Tx
    #
    #
    # work with standard transactions
    #
    # Standard transactions provide so-called "1.5 phase commit".  We
    # can ensure that work is never lost, but there is a chance of
    # confirmations being lost, so that messages may be resent.
    # Applications that use standard transactions must be able to
    # detect and ignore duplicate messages.
    #
    # GRAMMAR:
    #
    #     tx                  = C:SELECT S:SELECT-OK
    #                         / C:COMMIT S:COMMIT-OK
    #                         / C:ROLLBACK S:ROLLBACK-OK
    #
    # RULE:
    #
    #     An client using standard transactions SHOULD be able to
    #     track all messages received within a reasonable period, and
    #     thus detect and reject duplicates of the same message. It
    #     SHOULD NOT pass these to the application layer.
    #
    #

    def tx_commit(self):
        """
        commit the current transaction

        This method commits all messages published and acknowledged in
        the current transaction.  A new transaction starts immediately
        after a commit.

        """
        self.send_method_frame((90, 20))
        return self.wait(allowed_methods=[
                          (90, 21),    # Channel.tx_commit_ok
                        ])


    def _tx_commit_ok(self, args):
        """
        confirm a successful commit

        This method confirms to the client that the commit succeeded.
        Note that if a commit fails, the server raises a channel
        exception.

        """
        pass


    def tx_rollback(self):
        """
        abandon the current transaction

        This method abandons all messages published and acknowledged
        in the current transaction.  A new transaction starts
        immediately after a rollback.

        """
        self.send_method_frame((90, 30))
        return self.wait(allowed_methods=[
                          (90, 31),    # Channel.tx_rollback_ok
                        ])


    def _tx_rollback_ok(self, args):
        """
        confirm a successful rollback

        This method confirms to the client that the rollback
        succeeded. Note that if an rollback fails, the server raises a
        channel exception.

        """
        pass


    def tx_select(self):
        """
        select standard transaction mode

        This method sets the channel to use standard transactions.
        The client must use this method at least once on a channel
        before using the Commit or Rollback methods.

        """
        self.send_method_frame((90, 10))
        return self.wait(allowed_methods=[
                          (90, 11),    # Channel.tx_select_ok
                        ])


    def _tx_select_ok(self, args):
        """
        confirm transaction mode

        This method confirms to the client that the channel was
        successfully set to use standard transactions.

        """
        pass


    #############
    #
    #  Dtx
    #
    #
    # work with distributed transactions
    #
    # Distributed transactions provide so-called "2-phase commit".
    # The AMQP distributed transaction model supports the X-Open XA
    # architecture and other distributed transaction implementations.
    # The Dtx class assumes that the server has a private
    # communications channel (not AMQP) to a distributed transaction
    # coordinator.
    #
    # GRAMMAR:
    #
    #     dtx                 = C:SELECT S:SELECT-OK
    #                           C:START S:START-OK
    #
    #

    def dtx_select(self):
        """
        select standard transaction mode

        This method sets the channel to use distributed transactions.
        The client must use this method at least once on a channel
        before using the Start method.

        """
        self.send_method_frame((100, 10))
        return self.wait(allowed_methods=[
                          (100, 11),    # Channel.dtx_select_ok
                        ])


    def _dtx_select_ok(self, args):
        """
        confirm transaction mode

        This method confirms to the client that the channel was
        successfully set to use distributed transactions.

        """
        pass


    def dtx_start(self, dtx_identifier):
        """
        start a new distributed transaction

        This method starts a new distributed transaction.  This must
        be the first method on a new channel that uses the distributed
        transaction mode, before any methods that publish or consume
        messages.

        PARAMETERS:
            dtx_identifier: shortstr

                transaction identifier

                The distributed transaction key. This identifies the
                transaction so that the AMQP server can coordinate
                with the distributed transaction coordinator.

        """
        args = AMQPWriter()
        args.write_shortstr(dtx_identifier)
        self.send_method_frame((100, 20), args)
        return self.wait(allowed_methods=[
                          (100, 21),    # Channel.dtx_start_ok
                        ])


    def _dtx_start_ok(self, args):
        """
        confirm the start of a new distributed transaction

        This method confirms to the client that the transaction
        started. Note that if a start fails, the server raises a
        channel exception.

        """
        pass


    #############
    #
    #  Test
    #
    #
    # test functional primitives of the implementation
    #
    # The test class provides methods for a peer to test the basic
    # operational correctness of another peer. The test methods are
    # intended to ensure that all peers respect at least the basic
    # elements of the protocol, such as frame and content organisation
    # and field types. We assume that a specially-designed peer, a
    # "monitor client" would perform such tests.
    #
    # GRAMMAR:
    #
    #     test                = C:INTEGER S:INTEGER-OK
    #                         / S:INTEGER C:INTEGER-OK
    #                         / C:STRING S:STRING-OK
    #                         / S:STRING C:STRING-OK
    #                         / C:TABLE S:TABLE-OK
    #                         / S:TABLE C:TABLE-OK
    #                         / C:CONTENT S:CONTENT-OK
    #                         / S:CONTENT C:CONTENT-OK
    #
    #

    def test_content(self, msg):
        """
        test content handling

        This method tests the peer's capability to correctly marshal
        content.

        """
        self.send_method_frame((120, 40))
        return self.wait(allowed_methods=[
                          (120, 41),    # Channel.test_content_ok
                        ])


    def _test_content(self, args):
        """
        test content handling

        This method tests the peer's capability to correctly marshal
        content.

        """
        msg = self.wait()


    def test_content_ok(self, msg, content_checksum):
        """
        report content test result

        This method reports the result of a Content method.  It
        contains the content checksum and echoes the original content
        as provided.

        PARAMETERS:
            content_checksum: long

                content hash

                The 32-bit checksum of the content, calculated by
                adding the content into a 32-bit accumulator.

        """
        args = AMQPWriter()
        args.write_long(content_checksum)
        self.send_method_frame((120, 41), args)
        return self.wait(allowed_methods=[
                        ])


    def _test_content_ok(self, args):
        """
        report content test result

        This method reports the result of a Content method.  It
        contains the content checksum and echoes the original content
        as provided.

        PARAMETERS:
            content_checksum: long

                content hash

                The 32-bit checksum of the content, calculated by
                adding the content into a 32-bit accumulator.

        """
        content_checksum = args.read_long()
        msg = self.wait()


    def test_integer(self, integer_1, integer_2, integer_3, integer_4, operation):
        """
        test integer handling

        This method tests the peer's capability to correctly marshal
        integer data.

        PARAMETERS:
            integer_1: octet

                octet test value

                An octet integer test value.

            integer_2: short

                short test value

                A short integer test value.

            integer_3: long

                long test value

                A long integer test value.

            integer_4: longlong

                long-long test value

                A long long integer test value.

            operation: octet

                operation to test

                The client must execute this operation on the provided
                integer test fields and return the result.

        """
        args = AMQPWriter()
        args.write_octet(integer_1)
        args.write_short(integer_2)
        args.write_long(integer_3)
        args.write_longlong(integer_4)
        args.write_octet(operation)
        self.send_method_frame((120, 10), args)
        return self.wait(allowed_methods=[
                          (120, 11),    # Channel.test_integer_ok
                        ])


    def _test_integer(self, args):
        """
        test integer handling

        This method tests the peer's capability to correctly marshal
        integer data.

        PARAMETERS:
            integer_1: octet

                octet test value

                An octet integer test value.

            integer_2: short

                short test value

                A short integer test value.

            integer_3: long

                long test value

                A long integer test value.

            integer_4: longlong

                long-long test value

                A long long integer test value.

            operation: octet

                operation to test

                The client must execute this operation on the provided
                integer test fields and return the result.

        """
        integer_1 = args.read_octet()
        integer_2 = args.read_short()
        integer_3 = args.read_long()
        integer_4 = args.read_longlong()
        operation = args.read_octet()


    def test_integer_ok(self, result):
        """
        report integer test result

        This method reports the result of an Integer method.

        PARAMETERS:
            result: longlong

                result value

                The result of the tested operation.

        """
        args = AMQPWriter()
        args.write_longlong(result)
        self.send_method_frame((120, 11), args)
        return self.wait(allowed_methods=[
                        ])


    def _test_integer_ok(self, args):
        """
        report integer test result

        This method reports the result of an Integer method.

        PARAMETERS:
            result: longlong

                result value

                The result of the tested operation.

        """
        result = args.read_longlong()


    def test_string(self, string_1, string_2, operation):
        """
        test string handling

        This method tests the peer's capability to correctly marshal
        string data.

        PARAMETERS:
            string_1: shortstr

                short string test value

                An short string test value.

            string_2: longstr

                long string test value

                A long string test value.

            operation: octet

                operation to test

                The client must execute this operation on the provided
                string test fields and return the result.

        """
        args = AMQPWriter()
        args.write_shortstr(string_1)
        args.write_longstr(string_2)
        args.write_octet(operation)
        self.send_method_frame((120, 20), args)
        return self.wait(allowed_methods=[
                          (120, 21),    # Channel.test_string_ok
                        ])


    def _test_string(self, args):
        """
        test string handling

        This method tests the peer's capability to correctly marshal
        string data.

        PARAMETERS:
            string_1: shortstr

                short string test value

                An short string test value.

            string_2: longstr

                long string test value

                A long string test value.

            operation: octet

                operation to test

                The client must execute this operation on the provided
                string test fields and return the result.

        """
        string_1 = args.read_shortstr()
        string_2 = args.read_longstr()
        operation = args.read_octet()


    def test_string_ok(self, result):
        """
        report string test result

        This method reports the result of a String method.

        PARAMETERS:
            result: longstr

                result value

                The result of the tested operation.

        """
        args = AMQPWriter()
        args.write_longstr(result)
        self.send_method_frame((120, 21), args)
        return self.wait(allowed_methods=[
                        ])


    def _test_string_ok(self, args):
        """
        report string test result

        This method reports the result of a String method.

        PARAMETERS:
            result: longstr

                result value

                The result of the tested operation.

        """
        result = args.read_longstr()


    def test_table(self, table, integer_op, string_op):
        """
        test field table handling

        This method tests the peer's capability to correctly marshal
        field table data.

        PARAMETERS:
            table: table

                field table of test values

                A field table of test values.

            integer_op: octet

                operation to test on integers

                The client must execute this operation on the provided
                field table integer values and return the result.

            string_op: octet

                operation to test on strings

                The client must execute this operation on the provided
                field table string values and return the result.

        """
        args = AMQPWriter()
        args.write_table(table)
        args.write_octet(integer_op)
        args.write_octet(string_op)
        self.send_method_frame((120, 30), args)
        return self.wait(allowed_methods=[
                          (120, 31),    # Channel.test_table_ok
                        ])


    def _test_table(self, args):
        """
        test field table handling

        This method tests the peer's capability to correctly marshal
        field table data.

        PARAMETERS:
            table: table

                field table of test values

                A field table of test values.

            integer_op: octet

                operation to test on integers

                The client must execute this operation on the provided
                field table integer values and return the result.

            string_op: octet

                operation to test on strings

                The client must execute this operation on the provided
                field table string values and return the result.

        """
        table = args.read_table()
        integer_op = args.read_octet()
        string_op = args.read_octet()


    def test_table_ok(self, integer_result, string_result):
        """
        report table test result

        This method reports the result of a Table method.

        PARAMETERS:
            integer_result: longlong

                integer result value

                The result of the tested integer operation.

            string_result: longstr

                string result value

                The result of the tested string operation.

        """
        args = AMQPWriter()
        args.write_longlong(integer_result)
        args.write_longstr(string_result)
        self.send_method_frame((120, 31), args)
        return self.wait(allowed_methods=[
                        ])


    def _test_table_ok(self, args):
        """
        report table test result

        This method reports the result of a Table method.

        PARAMETERS:
            integer_result: longlong

                integer result value

                The result of the tested integer operation.

            string_result: longstr

                string result value

                The result of the tested string operation.

        """
        integer_result = args.read_longlong()
        string_result = args.read_longstr()

    _METHOD_MAP = {
        (20, 11): _open_ok,
        (20, 20): _flow,
        (20, 21): _flow_ok,
        (20, 30): _alert,
        (20, 40): _close,
        (20, 41): _close_ok,
        (30, 11): _access_request_ok,
        (40, 11): _exchange_declare_ok,
        (40, 21): _exchange_delete_ok,
        (50, 11): _queue_declare_ok,
        (50, 21): _queue_bind_ok,
        (50, 31): _queue_purge_ok,
        (50, 41): _queue_delete_ok,
        (60, 11): _basic_qos_ok,
        (60, 21): _basic_consume_ok,
        (60, 31): _basic_cancel_ok,
        (60, 50): _basic_return,
        (60, 60): _basic_deliver,
        (60, 71): _basic_get_ok,
        (60, 72): _basic_get_empty,
        (90, 11): _tx_select_ok,
        (90, 21): _tx_commit_ok,
        (90, 31): _tx_rollback_ok,
        }

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
            payload = raw_data[7:size + 7]
            _frame_end = raw_data[7 + size:7 + size + 1]
            if _frame_end != self.FRAME_END:
                raise Exception('Framing Error, received 0x%02x while expecting 0xce' % ord(_frame_end))
            remaining_data = raw_data[7 + size + 1:]
            frame = (frame_type, channel, payload,)
            return frame, remaining_data

        raw_data = data # keep original bytes around (not utilized though).
        while len(raw_data) > 1:
            frame, raw_data = _extractFrame(raw_data)
            self.frameReceived(frame)

    def sendFrame(self, frame):
        """
        Encode frame components, and write binary data to transport.
        The payload should already be encoded.
        @ivar frame: A tuple of the form: (type, channel, payload)
        @type frame: C{tuple}
        """
        frame_type, channel, payload = frame
        size = len(payload)
        data = pack('>BHI%dsB' % size, frame_type, channel, size, payload, 0xce)
        self.transport.write(data)
        self.frame_send_buffer.get().addCallback(self.sendFrame)


    def frameReceived(self, frame):
        """
        Override this.
        @ivar frame: A tuple of the form: (type, channel, payload)
        @type frame: C{tuple}
        """
        raise NotImplementedError

class Connection(BinaryFrameTransceiver, ConnectionHandler):
    """
    """

    channel_max = 65535 # 2**16
    frame_max = 4096 # 2**12
    heartbeat = 0 # seconds (0 means no heartbeat)

    _channel = Channel
    _channel_count = 0
    channels = {}

    def __init__(self, vhost='/', username='guest', password='guest'):
        """
        """
        # self.frame_receive_buffer = defer.DeferredQueue()
        self.frame_send_buffer = defer.DeferredQueue()
        self.coop = task.Cooperator(started=False)
        ConnectionHandler.__init__(self, self, 0)
        # do_send_frame = dispatch_iterator(self.frame_send_buffer)
        # do_buf_iter = buffer_iterator(do_send_frame, self.sendFrame)
        # self.coop.coiterate(do_buf_iter) 
        self.frame_send_buffer.get().addCallback(self.sendFrame)

        self.vhost = vhost
        self.username = username
        self.password = password
        login = AMQPWriter()
        login.write_table({'LOGIN':username, 'PASSWORD':password})
        self.login = login.getvalue()[4:]

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
            chan.frame_receive_buffer.put(frame)
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



