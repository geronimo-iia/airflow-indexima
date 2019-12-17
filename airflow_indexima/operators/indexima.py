"""Indexima operators module definition."""
import datetime
from typing import Optional, Union

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_indexima.connection import ConnectionDecorator
from airflow_indexima.hooks.indexima import IndeximaHook


__all__ = ['IndeximaHookBasedOperator', 'IndeximaQueryRunnerOperator', 'IndeximaLoadDataOperator']


class IndeximaHookBasedOperator(BaseOperator):
    """Our base class for indexima operator.

    This class act as a wrapper on IndeximaHook.
    """

    ui_color = '#ededed'  # Define color for airflow UI.

    @apply_defaults
    def __init__(
        self,
        task_id: str,
        indexima_conn_id: str,
        connection_decorator: Optional[ConnectionDecorator] = None,
        dry_run: Optional[bool] = False,
        auth: Optional[str] = None,
        kerberos_service_name: Optional[str] = None,
        timeout_seconds: Optional[Union[int, datetime.timedelta]] = None,
        socket_keepalive: Optional[bool] = None,
        *args,
        **kwargs,
    ):
        """Create IndeximaHookBasedOperator instance.

        # Parameters
            task_id (str): task identifier
            indexima_conn_id (str): indexima connection identifier
            connection_decorator Optional[ConnectionDecorator]: optional connection decorator
            dry_run (Optional[bool]): dry run mode (default: False). If true no action will
                be applied against datasource.
            auth (Optional[str]): authentication mode (default: {'CUSTOM'})
            kerberos_service_name (Optional[str]): optional kerberos service name
            timeout_seconds (Optional[Union[int, datetime.timedelta]]): define the socket timeout in second
                (could be an int or a timedelta)
            socket_keepalive (Optional[bool]): enable TCP keepalive.

        """
        super(IndeximaHookBasedOperator, self).__init__(task_id=task_id, *args, **kwargs)

        # align timeout_seconds on execution_timeout if setted
        if kwargs and 'execution_timeout' in kwargs and timeout_seconds is None:
            timeout_seconds = kwargs['execution_timeout']

        self._hook = IndeximaHook(
            indexima_conn_id=indexima_conn_id,
            connection_decorator=connection_decorator,
            dry_run=dry_run,
            auth=auth,
            kerberos_service_name=kerberos_service_name,
            timeout_seconds=timeout_seconds,
            socket_keepalive=socket_keepalive,
        )

    def get_hook(self) -> IndeximaHook:
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
        connection_decorator: Optional[ConnectionDecorator] = None,
        dry_run: Optional[bool] = False,
        auth: Optional[str] = None,
        kerberos_service_name: Optional[str] = None,
        timeout_seconds: Optional[Union[int, datetime.timedelta]] = None,
        socket_keepalive: Optional[bool] = None,
        *args,
        **kwargs,
    ):
        """Create IndeximaQueryRunnerOperator instance.

        # Parameters
            task_id (str): task identifier
            sql_query (str): query to run
            indexima_conn_id (str): indexima connection identifier
            connection_decorator Optional[ConnectionDecorator]: optional connection decorator
            dry_run (Optional[bool]): dry run mode (default: False). If true no action will
                be applied against datasource.
            auth (Optional[str]): authentication mode (default: {'CUSTOM'})
            kerberos_service_name (Optional[str]): optional kerberos service name
            timeout_seconds (Optional[Union[int, datetime.timedelta]]): define the socket timeout in second
                (could be an int or a timedelta)
            socket_keepalive (Optional[bool]): enable TCP keepalive.

        """
        super(IndeximaQueryRunnerOperator, self).__init__(
            task_id=task_id,
            indexima_conn_id=indexima_conn_id,
            connection_decorator=connection_decorator,
            dry_run=dry_run,
            auth=auth,
            kerberos_service_name=kerberos_service_name,
            timeout_seconds=timeout_seconds,
            socket_keepalive=socket_keepalive,
            *args,
            **kwargs,
        )
        self._sql_query = sql_query

    def execute(self, context):
        """Execute sql query.

        # Parameters
            context: dag context
        """
        self.get_hook().run(self._sql_query)


class IndeximaLoadDataOperator(IndeximaHookBasedOperator):
    r"""Indexima load data operator.

    Operations:

        1. truncate target_table (false per default)
        2. load source_select_query into target_table using redshift_user_name credential
        4. commit/rollback target_table

    All fields ('target_table', 'load_path_uri', 'source_select_query', 'truncate_sql',
    'format_query', 'prefix_query', 'skip_lines', 'no_check', 'limit', 'locale',
    'pause_delay_in_seconds_between_query' ) support airflow macro.

    Syntax (see https://indexima.com/support/doc/v.1.7/Load_Data/Load_Data_Inpath.html)
    ```sql
    LOAD DATA INPATH 'path_of_the_data_source'
    INTO TABLE my_data_space
    [FORMAT 'separator' / ORC / PARQUET / JSON];
    [PREFIX 'value1 \t value2 \t ... \t']
    [QUERY "my_SQL_Query"]
    [SKIP lines]
    [NOCHECK]
    [LIMIT n_lines]
    [LOCALE 'FR']
    ```

    """

    template_fields = (
        '_target_table',
        '_load_path_uri',
        '_source_select_query',
        '_truncate_sql',
        '_format_query',
        '_prefix_query',
        '_skip_lines',
        '_no_check',
        '_limit',
        '_locale',
        '_pause_delay_in_seconds_between_query',
    )

    def __init__(
        self,
        task_id: str,
        indexima_conn_id: str,
        target_table: str,
        load_path_uri: str,
        truncate: bool = False,
        truncate_sql: Optional[str] = None,
        connection_decorator: Optional[ConnectionDecorator] = None,
        source_select_query: Optional[str] = None,
        format_query: Optional[str] = None,
        prefix_query: Optional[str] = None,
        skip_lines: Optional[int] = None,
        no_check: Optional[bool] = False,
        limit: Optional[int] = None,
        locale: Optional[str] = None,
        dry_run: Optional[bool] = False,
        auth: Optional[str] = None,
        kerberos_service_name: Optional[str] = None,
        timeout_seconds: Optional[Union[int, datetime.timedelta]] = None,
        socket_keepalive: Optional[bool] = None,
        pause_delay_in_seconds_between_query: Optional[int] = None,
        *args,
        **kwargs,
    ):
        """Create IndeximaLoadDataOperator instance.

        # Parameters
            task_id (str): task identifier
            indexima_conn_id (str): indexima connection identifier
            target_table (str): target table to load into
            load_path_uri (str): source uri
            truncate (bool): if true execute truncate query before load (default: False)
            truncate_sql (Optional[str]): truncate query (truncate table per default)
            connection_decorator Optional[ConnectionDecorator]: optional connection decorator
            source_select_query (Optional[str]): optional sql query to select data from load_path_uri
            format_query (Optional[str]): optional format to identify a character separator or a file format
                tailored for big-data ecosystems
            prefix_query (Optional[str]): optional prefix used to identify a subset of data to be imported.
            skip_lines (Optional[int]): Allows to skip headers lines when importing data for flat text files.
            no_check (Optional[bool]): Do not validate data type when loading data when specified
                (default: False)
            limit (Optional[int]): It will import only the first #lines on each imported files.
            locale (Optional[str]): The LOCALE 'country' parameter helps to convert character.
            dry_run (Optional[bool]): dry run mode (default: False). If true no action will
                be applied against datasource.
            auth (Optional[str]): authentication mode (default: {'CUSTOM'})
            timeout_seconds (Optional[Union[int, datetime.timedelta]]): define the socket timeout in second
                (could be an int or a timedelta)
            socket_keepalive (Optional[bool]): enable TCP keepalive.
            kerberos_service_name (Optional[str]): optional kerberos service name
            pause_delay_in_seconds_between_query (Optional[int]): optional pause delay between queries
                truncate, load and commit. A None, zero or negative value disable the 'pause'.
        """

        super(IndeximaLoadDataOperator, self).__init__(
            task_id=task_id,
            indexima_conn_id=indexima_conn_id,
            connection_decorator=connection_decorator,
            dry_run=dry_run,
            auth=auth,
            kerberos_service_name=kerberos_service_name,
            timeout_seconds=timeout_seconds,
            socket_keepalive=socket_keepalive,
            *args,
            **kwargs,
        )
        self._target_table = target_table
        self._load_path_uri = load_path_uri
        self._truncate = truncate
        self._truncate_sql = truncate_sql if truncate_sql else f'truncate table {self._target_table};'
        self._source_select_query = source_select_query
        self._format_query = format_query
        self._prefix_query = prefix_query
        self._skip_lines = skip_lines
        self._no_check = no_check
        self._limit = limit
        self._locale = locale
        self._pause_delay_in_seconds_between_query = pause_delay_in_seconds_between_query

    def generate_load_data_query(self) -> str:
        """Generate 'load data' sql query.

        # Returns
            (str): load data sql query
        """

        def escape_quote(txt: str) -> str:
            return txt.replace("'", "\\'")

        sql_query = [f"LOAD DATA INPATH '{self._load_path_uri}'", f"INTO TABLE {self._target_table}"]
        if self._format_query:
            sql_query.append(f"FORMAT {self._format_query}")
        if self._prefix_query:
            sql_query.append(f"PREFIX '{escape_quote(self._prefix_query)}'")
        if self._source_select_query:
            sql_query.append(f"QUERY '{escape_quote(self._source_select_query)}'")
        if self._skip_lines:
            sql_query.append(f"SKIP {self._skip_lines}")
        if self._no_check:
            sql_query.append(f"NOCHECK")
        if self._limit:
            sql_query.append(f"LIMIT {self._limit}")
        if self._locale:
            sql_query.append(f"LOCALE '{self._locale}'")

        return " ".join(sql_query) + ";"

    def _execute_pause(self, hook: IndeximaHook):
        if self._pause_delay_in_seconds_between_query and self._pause_delay_in_seconds_between_query > 0:
            hook.pause(self._pause_delay_in_seconds_between_query)

    def execute(self, context):
        """Process executor."""
        try:
            with self.get_hook() as hook:
                if self._truncate and self._truncate_sql:
                    hook.run(self._truncate_sql)
                    self._execute_pause(hook=hook)

                cursor = hook.run(self.generate_load_data_query())
                hook.check_error_of_load_query(cursor=cursor)

                self._execute_pause(hook=hook)

                hook.commit(tablename=self._target_table)
        except Exception as e:
            self.log.error(e)

            with self.get_hook() as hook:
                self._execute_pause(hook=hook)
                hook.rollback(tablename=self._target_table)

            raise e
