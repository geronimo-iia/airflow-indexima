"""Indexima operator definition."""

from typing import Optional
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_indexima.hook import IndeximaHook


__all__ = ['IndeximaQueryRunnerOperator', 'IndeximaHookBasedOperator']


class IndeximaHookBasedOperator(BaseOperator):

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self, task_id: str, indexima_conn_id: str, auth: str = 'CUSTOM', *args, **kwargs,
    ):
        super(IndeximaHookBasedOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.indexima_conn_id = indexima_conn_id
        self.auth = auth

    def get_hook(self):
        return IndeximaHook(indexima_conn_id=self.indexima_conn_id, auth=self.auth,)


class IndeximaQueryRunnerOperator(IndeximaHookBasedOperator):

    template_fields: tuple = ('sql_query',)

    @apply_defaults
    def __init__(
        self, task_id: str, sql_query: str, indexima_conn_id: str, auth: str = 'CUSTOM', *args, **kwargs,
    ):
        super(IndeximaQueryRunnerOperator, self).__init__(
            task_id=task_id,
            indexima_conn_id=indexima_conn_id,
            username=username,
            password_key=password_key,
            auth=auth,
            *args,
            **kwargs,
        )
        self.sql_query = sql_query

    def execute(self, context):
        self.get_hook().run(self.sql_query)


class IndeximaLoadDataOperator(IndeximaHookBasedOperator):
    """Redshift to Indexima with mode full (truncate and import).

    Operations:
        1. truncate target_table (false per default)
        2. load source_select_query into target_table using redshift_user_name credential
        3. commit/rollback target_table
    """

    template_fields = ('load_path_uri', 'source_select_query', 'truncate_sql')

    def __init__(
        self,
        task_id: str,
        indexima_conn_id: str,
        target_table: str,
        source_select_query: str,
        load_path_uri: str,
        truncate: bool = False,
        truncate_sql: Optional[str] = None,
        auth: str = 'CUSTOM',
        *args,
        **kwargs,
    ):
        """Create IndeximaLoadDataOperator instance.

        # Parameters
            task_id (str): task identifier
            indexima_conn_id (str): indexima connection identifier
            target_table (str): target table to load into
            source_select_query (str): sql query to select data from load_path_uri
            load_path_uri (str): source uri
            truncate (bool): if true execute truncate query before load (default: False)
            truncate_sql (Optional[str]): truncate query (truncate table per default)
            auth (str): authentication mode (default: {'CUSTOM'})

        """

        super(IndeximaLoadDataOperator, self).__init__(
            task_id=task_id, indexima_conn_id=indexima_conn_id, auth=auth, *args, **kwargs,
        )
        self.target_table = target_table
        self.source_select_query = source_select_query
        self.load_path_uri = load_path_uri
        self.truncate = truncate
        self.truncate_sql = truncate_sql if truncate_sql else f'truncate table {self.target_table}'

    def execute(self, context):
        with self.get_hook() as hook:
            if self.truncate and self.truncate_sql:
                hook.run(self.truncate_sql)
            try:
                hook.run(
                    f"load data inpath '{self.load_path_uri}' "  # noqa= E501
                    f"into table {self.target_table} query '{self.source_select_query}';"
                )
                hook.commit(tablename=self.target_table)
            except Exception as e:
                hook.rollback(tablename=self.target_table)
                raise e
