from urllib.parse import urlparse
from .connection import Connection
from .exceptions import NoAvailableConnectionsInPoolError
from .protocol import MongoProtocol
import asyncio


__all__ = ('Pool', )


class Pool:
    """
    Pool of connections. Each
    Takes care of setting up the connection and connection pooling.

    When pool_size > 1 and some connections are in use because of transactions
    or blocking requests, the other are preferred.

    ::

        pool = yield from Pool.create(host='localhost', port=6379, pool_size=10)
        result = yield from connection.set('key', 'value')
    """

    protocol = MongoProtocol
    """
    The :class:`MongoProtocol` class to be used for each connection in this pool.
    """

    @classmethod
    def get_connection_class(cls):
        """
        Return the :class:`Connection` class to be used for every connection in
        this pool. Normally this is just a ``Connection`` using the defined ``protocol``.
        """
        class ConnectionClass(Connection):
            protocol = cls.protocol
        return ConnectionClass

    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=27017, db=None, username=None, password=None, url=None, loop=None,
               poolsize=1, auto_reconnect=True):
        """
        Create a new pool instance.
        """
        self = cls()

        if url:
            url = urlparse(url)

            try:
                db = url.path.replace('/', '')
            except (AttributeError, ValueError):
                raise Exception("Missing database name in URI")

            host = url.hostname

            try:
                port = url.port
            except ValueError:
                port = url._hostinfo[1]
                # strip any additional hosts in the connection string for now
                try:
                    if "," in port:
                        hosts = port.split(",")
                        netloc = hosts[len(hosts)-1]
                        netloc = netloc.split(':')
                        host = netloc[0]
                        port = netloc[1]
                except TypeError:
                    pass

            self._host = host
            self._port = port or 27017
            username = url.username
            password = url.password
        else:
            self._host = host
            self._port = port

        self._pool_size = poolsize

        # Create connections
        self._connections = []

        for i in range(poolsize):
            connection_class = cls.get_connection_class()
            connection = yield from connection_class.create(host=self._host, port=self._port, db=db, username=username,
                                                            password=password, loop=loop,
                                                            auto_reconnect=auto_reconnect)
            self._connections.append(connection)

        return self

    def __repr__(self):
        return 'Pool(host=%r, port=%r, pool_size=%r)' % (self._host, self._port, self._poolsize)

    @property
    def pool_size(self):
        """ Number of parallel connections in the pool."""
        return self._poolsize

    @property
    def connections_connected(self):
        """
        The amount of open TCP connections.
        """
        return sum([1 for c in self._connections if c.protocol.is_connected])

    def close(self):
        for conn in self._connections:
            conn.disconnect()

    def _get_free_connection(self):
        """
        Return the next protocol instance that's not in use.
        (A protocol in pubsub mode or doing a blocking request is considered busy,
        and can't be used for anything else.)
        """
        self._shuffle_connections()

        for c in self._connections:
            if c.protocol.is_connected:
                return c

    def _shuffle_connections(self):
        """
        'shuffle' protocols. Make sure that we divide the load equally among the protocols.
        """
        self._connections = self._connections[1:] + self._connections[:1]

    def __getattr__(self, name):
        """
        Proxy to a protocol. (This will choose a protocol instance that's not
        busy in a blocking request or transaction.)
        """

        connection = self._get_free_connection()

        if connection:
            return getattr(connection, name)
        else:
            raise NoAvailableConnectionsInPoolError('No available connections in the pool: size=%s, connected=%s' % (
                                                    self._pool_size, self.connections_connected))
