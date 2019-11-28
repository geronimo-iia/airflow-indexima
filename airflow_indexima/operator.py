"""Indexima operators module definition."""

from typing import Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_indexima.hook import IndeximaHook, PrepareConnectionHandler


__all__ = ['IndeximaQueryRunnerOperator', 'IndeximaHookBasedOperator']


class IndeximaHookBasedOperator(BaseOperator):
    """Our base class for indexima operator.

    if you would customize IndeximaHook, you could define another one with ```hook_class_name``` field.
    """

    hook_class_name = IndeximaHook

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        task_id: str,
        indexima_conn_id: str,
        auth: str = 'CUSTOM',
        prepare_connection: Optional[PrepareConnectionHandler] = None,
        *args,
        **kwargs,
    ):
        super(IndeximaHookBasedOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self._hook = IndeximaHook(
            indexima_conn_id=indexima_conn_id, auth=auth, prepare_connection=prepare_connection
        )

    def get_hook(self):
        """Return a configured IndeximaHook instance."""
        return self._hook


class IndeximaQueryRunnerOperator(IndeximaHookBasedOperator):
    """A simple query executor."""

    template_fields: tuple = ('_sql_query',)

    @apply_defaults
    def __init__(
        self,
        task_id: str,
        sql_query: str,
        indexima_conn_id: str,
        auth: str = 'CUSTOM',
        prepare_connection: Optional[PrepareConnectionHandler] = None,
        *args,
        **kwargs,
    ):
        super(IndeximaQueryRunnerOperator, self).__init__(
            task_id=task_id,
            indexima_conn_id=indexima_conn_id,
            auth=auth,
            prepare_connection=prepare_connection,
            *args,
            **kwargs,
        )
        self._sql_query = sql_query

    def execute(self, context):
        self.get_hook().run(self._sql_query)


class IndeximaLoadDataOperator(IndeximaHookBasedOperator):
    """Redshift to Indexima with mode full (truncate and import).

    Operations:

        1. truncate target_table (false per default)
        2. load source_select_query into target_table using redshift_user_name credential
        3. commit/rollback target_table

    All fields ('target_table', 'load_path_uri', 'source_select_query', 'truncate_sql') support airflow macro.
    """

    template_fields = ('_target_table', '_load_path_uri', '_source_select_query', '_truncate_sql')

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
        prepare_connection: Optional[PrepareConnectionHandler] = None,
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
            task_id=task_id,
            indexima_conn_id=indexima_conn_id,
            auth=auth,
            prepare_connection=prepare_connection,
            *args,
            **kwargs,
        )
        self._target_table = target_table
        self._source_select_query = source_select_query
        self._load_path_uri = load_path_uri
        self._truncate = truncate
        self._truncate_sql = truncate_sql if truncate_sql else f'truncate table {self.target_table}'

    def execute(self, context):
        with self.get_hook() as hook:
            if self._truncate and self._truncate_sql:
                hook.run(self._truncate_sql)
            try:
                hook.run(
                    f"load data inpath '{self._load_path_uri}' "
                    f"into table {self._target_table} query '{self._source_select_query}';"
                )
                hook.commit(tablename=self._target_table)
            except Exception as e:
                hook.rollback(tablename=self._target_table)
                raise e
