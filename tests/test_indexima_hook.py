import datetime

from airflow_indexima.hooks.indexima import IndeximaHook


def test_indexima_hook_settings(indexima_connection):
    hook = IndeximaHook(indexima_conn_id=indexima_connection.id)
    assert hook._hive_configuration == {'serialization.encoding': 'utf-8'}


def test_indexima_hook_decorator(indexima_connection):
    hook = IndeximaHook(indexima_conn_id=indexima_connection.id, timeout_seconds=10, socket_keepalive=True)
    conn = hook._settings_decorator(indexima_connection)
    assert conn.extra == '{"timeout_seconds": 10, "socket_keepalive": true}'


def test_indexima_timeout_settings_from_timedelta(indexima_connection):
    hook = IndeximaHook(indexima_conn_id=indexima_connection.id, timeout_seconds=datetime.timedelta(hours=10))
    conn = hook._settings_decorator(indexima_connection)
    assert conn.extra == '{"timeout_seconds": 36000}'
