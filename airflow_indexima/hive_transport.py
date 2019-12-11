"""Define hive transport function utilities.

PyHive supported authentication mode:

- NONE
- CUSTOM
- LDAP
- KERBEROS
- NOSASL

Extra configuration:

- socket timeout_seconds
- socket keepalive

"""
from typing import Optional

import sasl
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift_sasl import TSaslClientTransport


__all__ = [
    'HIVE_AUTH_MODES',
    'create_transport_socket',
    'create_hive_plain_transport',
    'create_hive_gssapi_transport',
    'create_hive_nosasl_transport',
    'check_hive_connection_parameters',
    'create_hive_transport',
]


HIVE_AUTH_MODES = ('NONE', 'CUSTOM', 'KERBEROS', 'NOSASL', 'LDAP')


def create_transport_socket(
    host: str,
    port: Optional[int],
    timeout_seconds: Optional[int] = None,
    socket_keepalive: Optional[bool] = None,
) -> TSocket:
    """Create a transport socket.

    This function expose TSocket configuration option (more for clarity rather than anything else).

    # Parameters
        host (str): The host to connect to.
        port (int): The (TCP) port to connect to.
        timeout_seconds (Optional[int]): define the socket timeout in second
        socket_keepalive (Optional[bool]): enable TCP keepalive, default False.

    # Returns
        (TSocket): transport socket instance.

    """
    socket = TSocket(
        host=host,
        port=port if port else 10000,
        socket_keepalive=socket_keepalive if socket_keepalive is not None else False,
    )
    if timeout_seconds:
        socket.setTimeout(timeout_seconds * 1000)  # set timeout in ms
    return socket


def create_hive_plain_transport(
    socket: TSocket, username: str, password: Optional[str] = None
) -> TSaslClientTransport:
    """Create a TSaslClientTransport in 'PLAIN' authentication mode.

    # Parameters
        socket (TSocket): socket to use
        username (str): username to login
        password (Optional[str]): optional password to login

    # Returns
        (TSaslClientTransport): transport instance

    """

    def _sasl_factory():
        sasl_client = sasl.Client()
        sasl_client.setAttr('host', socket.host)
        sasl_client.setAttr('username', username)
        if password:
            sasl_client.setAttr('password', password)
        sasl_client.init()
        return sasl_client

    return TSaslClientTransport(_sasl_factory, 'PLAIN', socket)


def create_hive_gssapi_transport(socket: TSocket, service_name: str) -> TSaslClientTransport:
    """Create a TSaslClientTransport in 'GSSAPI' authentication mode.

    # Parameters
        socket (TSocket): socket to use
        service_name (str): kerberos service name

    # Returns
        (TSaslClientTransport): transport instance

    """

    def _sasl_factory():
        sasl_client = sasl.Client()
        sasl_client.setAttr('host', socket.host)
        sasl_client.setAttr('service', service_name)
        sasl_client.init()
        return sasl_client

    return TSaslClientTransport(_sasl_factory, 'GSSAPI', socket)


def create_hive_nosasl_transport(socket: TSocket) -> TBufferedTransport:
    """Create a TBufferedTransport in 'NOSASL' authentication mode.

    NOSASL corresponds to hive.server2.authentication=NOSASL in hive-site.xml

    # Parameters
        socket (TSocket):  socket to use

    # Returns
        (TBufferedTransport): transport instance
    """
    return TBufferedTransport(socket)


def check_hive_connection_parameters(
    auth: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    kerberos_service_name: Optional[str] = None,
):
    """Check hive connection parameters.

    # Parameters
        auth (Optional[str]): authentication mode)
        username (Optional[str]): optional username to login
        password (Optional[str]): optional password to login
        kerberos_service_name (Optional[str]): optional service name

    # Raises
        (ValueError): if something is wrong
    """
    # username will be checked in hive.Connection

    if auth not in HIVE_AUTH_MODES:
        raise ValueError(f"Unknown auth parameter '{auth}' (use one of {HIVE_AUTH_MODES}).")

    if auth in ('LDAP', 'CUSTOM') and password is None:
        raise ValueError(
            "Password should be set in LDAP or CUSTOM mode; " "Remove password or use one of those modes"
        )
    if auth == 'KERBEROS' and kerberos_service_name is None:
        raise ValueError("kerberos_service_name should be set in KERBEROS mode")


def create_hive_transport(
    host: str,
    port: Optional[int] = None,
    timeout_seconds: Optional[int] = None,
    socket_keepalive: Optional[bool] = None,
    auth: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    kerberos_service_name: Optional[str] = None,
) -> TSaslClientTransport:
    """Create a TSaslClientTransport.

    Implementation is heavly based on pyhive.hive.Connection constructor.

    # Parameters
        host (str): The host to connect to.
        port (int): The (TCP) port to connect to.
        timeout_seconds (Optional[int]): define the socket timeout in second (default 60)
        socket_keepalive (Optional[bool]): enable TCP keepalive, default off.
        auth (Optional[str]): authentication mode (Defaul 'NONE')
        username (Optional[str]): optional username to login
        password (Optional[str]): optional password to login
        kerberos_service_name (Optional[str]): optional kerberos service name

    # Returns
        (TSaslClientTransport): transport instance

    # Raises
        (ValueError): if something is wrong
    """

    check_hive_connection_parameters(
        auth=auth, username=username, password=password, kerberos_service_name=kerberos_service_name
    )

    socket = create_transport_socket(
        host=host, port=port, timeout_seconds=timeout_seconds, socket_keepalive=socket_keepalive
    )

    if auth == 'KERBEROS' and kerberos_service_name:
        return create_hive_gssapi_transport(socket=socket, service_name=kerberos_service_name)

    if auth == 'NOSASL':
        return create_hive_nosasl_transport(socket=socket)

    if auth in ('CUSTOM', 'LDAP', 'NONE') and username:
        return create_hive_plain_transport(socket=socket, username=username, password=password)
