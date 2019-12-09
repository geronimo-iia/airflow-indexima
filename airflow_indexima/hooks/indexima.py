"""Indexima hook module definition."""

from typing import Any, List, Optional

from airflow.hooks.base_hook import BaseHook
from pyhive import hive

from airflow_indexima.connection import (
    ConnectionDecorator,
    apply_hive_extra_setting,
    extract_hive_extra_setting,
)
from airflow_indexima.hive_transport import create_hive_transport


__all__ = ['IndeximaHook']


class IndeximaHook(BaseHook):
    """Indexima hook implementation.

    This implementation can be used as a context manager like this:

    ```python
    with IndeximaHook(...) as hook:
        hook.run('select ...')
    ```

    This implementation can be customized with a ```connection_decorator```function
    which must have this profile: Callable[[Connection], Connection] (alias ConnectionDecorator)

    In this handler you could retreive credentials from other backeng like aws ssm.
    """

    def __init__(
        self,
        indexima_conn_id: str,
        connection_decorator: Optional[ConnectionDecorator] = None,
        dry_run: Optional[bool] = False,
        auth: Optional[str] = None,
        kerberos_service_name: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        socket_keepalive: Optional[bool] = None,
        *args,
        **kwargs,
    ):
        """Create an IndeximaHook instance.

        # Parameters
            indexima_conn_id(str): connection identifier
            auth(str): pyhive authentication mode (defaults: 'CUSTOM')
            connection_decorator (Optional[ConnectionDecorator]) : optional function handler
                to post process connection parameter(default: None)
            dry_run (Optional[bool]): dry run mode (default: False). If true no action will
                ve applied against datasource.
            timeout_seconds (Optional[int]): define the socket timeout in second
            socket_keepalive (Optional[bool]): enable TCP keepalive.
            kerberos_service_name (Optional[str]): optional kerberos service name

        """
        super(IndeximaHook, self).__init__(source='indexima', *args, **kwargs)
        self._indexima_conn_id = indexima_conn_id
        self._schema = kwargs.pop("schema", None)

        self._conn: Optional[Any] = None
        self._connection_decorator = connection_decorator
        self._dry_run = dry_run or False

        self._settings_decorator = lambda connection: apply_hive_extra_setting(
            connection=connection,
            auth=auth,
            kerberos_service_name=kerberos_service_name,
            timeout_seconds=timeout_seconds,
            socket_keepalive=socket_keepalive,
        )

    def get_conn(self) -> hive.Connection:
        """Return a hive connection.

        # Returns
            (hive.Connection): the hive connection
        """

        conn = self.get_connection(self._indexima_conn_id)
        if not conn:
            raise RuntimeError(f'no connection identifier found with {self._indexima_conn_id}')

        # load extra parameters of airflow connection
        conn = self._settings_decorator(conn)

        # apply decorator
        if self._connection_decorator:
            conn = self._connection_decorator(conn)

        self.log.info(f'connect to {conn.host}  {conn.login} {conn.port}')  # noqa: E501
        (auth, kerberos_service_name, timeout_seconds, socket_keepalive) = extract_hive_extra_setting(
            connection=conn
        )

        # build parameters for create_hive_transport and keep default value meaning
        parameters = {'host': conn.host}
        parameters['port'] = conn.port or 10000
        parameters['timeout_seconds'] = timeout_seconds or 60
        if socket_keepalive is not None:
            parameters['socket_keepalive'] = socket_keepalive
        parameters['auth'] = auth or 'CUSTOM'
        if conn.login:
            parameters['username'] = conn.login
        if conn.password:
            parameters['password'] = conn.password
        if kerberos_service_name:
            parameters['kerberos_service_name'] = kerberos_service_name
        # TODO test only
        configuration = {
            "hive.server.read.socket.timeout": str(3600000),
            "hive.server2.session.check.interval": str(3600000),
            "hive.server2.idle.session.check.operation": "true",
            "hive.server2.idle.operation.timeout": str(3600000 * 24),
            "hive.server2.idle.session.timeout": str(3600000 * 24 * 3)
        }
        self._conn = hive.Connection(
            configuration=configuration,
            database=self._schema or conn.schema, thrift_transport=create_hive_transport(**parameters)
        )
        return self._conn

    def get_records(self, sql: str) -> hive.Cursor:
        """Execute query and return curror.

        (alias of run method)
        """
        return self.run(sql=sql)

    def get_pandas_df(self, sql: str):
        raise NotImplementedError()

    def run(self, sql: str) -> hive.Cursor:
        """Execute query and return curror."""
        if not self._conn:
            self.get_conn()
        cursor = self._conn.cursor()  # type: ignore
        if not self._dry_run:
            cursor.execute(sql)
        else:
            self.log.warn(sql)
        return cursor

    def check_error_of_load_query(self, cursor: hive.Cursor):
        """Raise error if a load query fail.

        # Parameters
            cursor: cursor returned by load path query.

        # Raises
            (RuntimeError): if an error is found

        """
        _messages: List[str] = []
        for path, inserts, errors, message in iter(cursor.fetchone, None):  # type: ignore
            if errors > 0:  # type: ignore
                _messages.append(f"({path}, {inserts}, {errors}: {message}")  # type: ignore

        if len(_messages):
            raise RuntimeError('\n'.join(_messages))

    def commit(self, tablename: str):
        """Execute a simple commit on table.

        # Parameters
            tablename (str): table name to commit
        """
        self.run(f'COMMIT {tablename}')

    def rollback(self, tablename: str):
        """Execute a simple rollback on table.

        # Parameters
            tablename (str): table name to rollback
        """
        self.run(f'ROLLBACK {tablename}')

    def close(self):
        """Close current connection."""
        if self._conn:
            self._conn.close()
        self._conn = None

    def __enter__(self):
        self.get_conn()
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def is_dry_run(self) -> bool:
        return self._dry_run
