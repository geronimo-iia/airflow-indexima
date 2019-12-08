"""Define a decorator connection function profile.

Implementation can be used tp customized a connection like
retreive credentials from other backeng like aws ssm.

```ConnectionDecorator = Callable[[Connection], Connection]```

"""
import json
from typing import Callable, Optional, Tuple

from airflow.models import Connection


__all__ = ['ConnectionDecorator', 'apply_hive_extra_setting', 'extract_hive_extra_setting']

ConnectionDecorator = Callable[[Connection], Connection]


def apply_hive_extra_setting(
    connection: Connection,
    auth: Optional[str] = None,
    kerberos_service_name: Optional[str] = None,
    timeout_seconds: Optional[int] = None,
    socket_keepalive: Optional[bool] = None,
) -> Connection:
    """Apply extra settings on hive connection.

    # Parameters:
        connection (Connection): airflow connection
        auth (Optional[str]): optional authentication mode
        kerberos_service_name (Optional[str]): optional kerberos service name
        timeout_seconds (Optional[int]): optional define the socket timeout in second
        socket_keepalive (Optional[bool]): optional enable TCP keepalive.

    # Returns
        (Connection): configured airflow Connection instance
    """

    _extra = json.loads(connection.extra) if connection.extra else {}

    # ovveride if provided
    if auth:
        _extra['auth'] = auth
    if kerberos_service_name:
        _extra['kerberos_service_name'] = kerberos_service_name
    if timeout_seconds:
        _extra['timeout_seconds'] = timeout_seconds
    if socket_keepalive is not None:
        _extra['socket_keepalive'] = socket_keepalive

    connection.extra = json.dumps(_extra)

    return connection


def extract_hive_extra_setting(
    connection: Connection,
) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[bool]]:
    """Extract extra settings.

    # Parameters:
        connection (Connection): airflow connection

    # Returns
        (Tuple[Optional[str], Optional[str], Optional[int], Optional[bool]]): a tuple
            (auth, kerberos_service_name, timeout_seconds, socket_keepalive)
    """
    _extra = json.loads(connection.extra) if connection.extra else {}

    return (
        _extra['auth'] if 'auth' in _extra else None,
        _extra['kerberos_service_name'] if 'kerberos_service_name' in _extra else None,
        _extra['timeout_seconds'] if 'timeout_seconds' in _extra else None,
        _extra['socket_keepalive'] if 'socket_keepalive' in _extra else None,
    )
