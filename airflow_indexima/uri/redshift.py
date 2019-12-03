"""Define an uri generator for redshift."""

from typing import Optional

from airflow.hooks.base_hook import BaseHook

from airflow_indexima.connection import ConnectionDecorator


__all__ = ['get_redshift_load_path_uri']


def get_redshift_load_path_uri(connection_id: str, decorator: Optional[ConnectionDecorator] = None) -> str:
    """Return load path uri from a connection_id.

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
    conn = BaseHook.get_connection(connection_id)
    if not conn:
        raise RuntimeError(f'no connection with {connection_id}')
    if decorator:
        conn = decorator(conn)

    _result = (
        f":jdbc:redshift//{conn.host}:{conn.port}/{conn.schema}"
        f"?user={conn.login}"
        f"&password={conn.password}"
    )
    if conn.extra:
        for key in conn.extra:
            _result += f"&{key}={conn.extra[key]}"

    return _result
