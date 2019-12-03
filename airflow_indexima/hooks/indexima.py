"""Indexima hook module definition."""

from typing import Any, Optional

from airflow.hooks.base_hook import BaseHook
from pyhive import hive

from airflow_indexima.connection import ConnectionDecorator


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
        auth: str = 'CUSTOM',
        connection_decorator: Optional[ConnectionDecorator] = None,
        *args,
        **kwargs,
    ):
        """Create an IndeximaHook instance.

        # Parameters
            indexima_conn_id(str): connection identifier
            auth(str): pyhive authentication mode (defaults: 'CUSTOM')
            connection_decorator (Optional[ConnectionDecorator]) : optional function handler
                to post process connection parameter(default: None)
        """
        super(IndeximaHook, self).__init__(source='indexima', *args, **kwargs)
        self._indexima_conn_id = indexima_conn_id
        self._schema = kwargs.pop("schema", None)
        self._auth = auth
        self._conn: Optional[Any] = None
        self._connection_decorator = connection_decorator

    def get_conn(self) -> hive.Connection:
        """Return a hive connection.

        # Returns
            (hive.Connection): the hive connection
        """

        conn = self.get_connection(self._indexima_conn_id)
        if not conn:
            raise RuntimeError(f'no connection identifier found with {self._indexima_conn_id}')
        if self._connection_decorator:
            conn = self._connection_decorator(conn)
        self.log.info(
            f'connect to {conn.host}  {conn.login}  {"X"*len(conn.password)}  {conn.port} {self._auth}'  # noqa: E501
        )
        self._conn = hive.Connection(
            host=conn.host,
            username=conn.login,
            password=conn.password,
            database=self._schema or conn.schema,
            port=conn.port if conn.port else 10000,
            auth=self._auth,
        )
        return self._conn

    def get_records(self, sql: str):
        """Execute query and return curror.

        (alias of run method)
        """
        return self.run(sql=sql)

    def get_pandas_df(self, sql: str):
        raise NotImplementedError()

    def run(self, sql: str):
        """Execute query and return curror."""
        if not self._conn:
            self.get_conn()
        cursor = self._conn.cursor()  # type: ignore
        cursor.execute(sql)
        return cursor

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

    def __enter__(self):
        self.get_conn()
        return self

    def __exit__(self, *exc):
        if self._conn:
            self._conn.close()
        self._conn = None
        return False
