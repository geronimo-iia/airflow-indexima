"""Indexima hook module definition."""

from typing import Any, Optional

from airflow.hooks.base_hook import BaseHook
from pyhive import hive


__all__ = ['IndeximaHook']


class IndeximaHook(BaseHook):
    """Indexima hook implementation.

    This implementation can be used as a context manager like this:

    ```python
    with IndeximaHook(...) as hook:
        hook.run('select ...')
    ```
    """

    def __init__(self, indexima_conn_id: str, auth: str = 'CUSTOM', *args, **kwargs):
        super(IndeximaHook, self).__init__(source='indexima', *args, **kwargs)
        self.indexima_conn_id = indexima_conn_id
        self.schema = kwargs.pop("schema", None)
        self.auth = auth
        self._conn: Optional[Any] = None

    def get_conn(self):
        """Return a hive connection."""

        conn = self.get_connection(self.indexima_conn_id)
        if not conn:
            raise RuntimeError(f'no connection identifier found with {self.indexima_conn_id}')
        self.log.info(
            f'connect to {conn.host}  {conn.username}  {"X"*len(conn.password)}  {conn.port} {self.auth}'  # noqa: E501
        )
        conn = self.prepare_connection(conn)
        self._conn = hive.Connection(
            host=conn.host,
            username=conn.username,
            password=conn.password,
            database=self.schema or conn.schema,
            port=conn.port if conn.port else 10000,
            auth=self.auth,
        )
        return self._conn

    def prepare_connection(self, conn):
        """Prepare connection parameter.

        If you would get credential from other backend like ssm, override this method.

        # returns:
            conn: connection parameters
        """
        return conn

    def get_records(self, sql):
        """Execute query and return curror.

        (alias of run method)
        """
        return self.run(sql=sql)

    def get_pandas_df(self, sql):
        raise NotImplementedError()

    def run(self, sql):
        """Execute query and return curror."""
        if not self._conn:
            self.get_conn()
        cursor = self._conn.cursor()  # type: ignore
        cursor.execute(sql)
        return cursor

    def commit(self, tablename):
        """Execute a simple commit on table."""
        self.run(f'COMMIT {tablename}')

    def rollback(self, tablename):
        """Execute a simple rollback on table."""
        self.run(f'ROLLBACK {tablename}')

    def __enter__(self):
        self.get_conn()
        return self

    def __exit__(self, *exc):
        if self._conn:
            self._conn.close()
        self._conn = None
        return False
