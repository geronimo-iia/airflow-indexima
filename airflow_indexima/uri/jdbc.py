"""Define an uri generator for redshift."""
import json
from typing import Optional

from airflow.hooks.base_hook import BaseHook

from airflow_indexima.connection import ConnectionDecorator


__all__ = ['get_jdbc_load_path_uri', 'get_redshift_load_path_uri', 'get_postgresql_load_path_uri']


def get_jdbc_load_path_uri(
    jdbc_type: str, connection_id: str, decorator: Optional[ConnectionDecorator] = None
) -> str:
    """Return jdbc load path uri from a connection_id.

    # Parameters
        jdbc_type (str): jdbc connection type
        connection_id (str): source connection identifier
        decorator (Optional[ConnectionDecorator]): optinal connection decorator

    # Returns
        (str) load path uri

    """
    conn = BaseHook.get_connection(connection_id)
    if not conn:
        raise RuntimeError(f'no connection with {connection_id}')
    if decorator:
        conn = decorator(conn)

    _result = (
        f"jdbc:{jdbc_type}://{conn.host}:{conn.port}/{conn.schema}"
        f"?user={conn.login}"
        f"&password={conn.password}"
    )
    if conn.extra:
        _extra = json.loads(conn.extra)
        for key in _extra:
            _result += f"&{key}={_extra[key]}"

    return _result


def get_redshift_load_path_uri(connection_id: str, decorator: Optional[ConnectionDecorator] = None) -> str:
    """Return redshift load path uri from a connection_id.

    Example:
    ```
        get_redshift_load_path_uri(connection_id='my_conn')
        >> 'jdbc:redshift://my-db:5439/db_client?ssl=true&user=airflow-user&password=XXXXXXXX'
    ```

    # Parameters
        connection_id (str): source connection identifier
        decorator (Optional[ConnectionDecorator]): optinal connection decorator

    # Returns
        (str) load path uri

    """
    return get_jdbc_load_path_uri(jdbc_type='redshift', connection_id=connection_id, decorator=decorator)


def get_postgresql_load_path_uri(connection_id: str, decorator: Optional[ConnectionDecorator] = None) -> str:
    """Return postgresql load path uri from a connection_id.

    Example:
    ```
        get_postgresql_load_path_uri(connection_id='my_conn')
        >> 'jdbc:postgresql://my-db:5432/db_client?ssl=true&user=airflow-user&password=XXXXXXXX'
    ```

    # Parameters
        connection_id (str): source connection identifier
        decorator (Optional[ConnectionDecorator]): optinal connection decorator

    # Returns
        (str) load path uri

    """
    return get_jdbc_load_path_uri(jdbc_type='postgresql', connection_id=connection_id, decorator=decorator)
