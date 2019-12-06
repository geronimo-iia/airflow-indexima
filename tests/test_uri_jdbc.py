from airflow.models import Connection

from airflow_indexima.uri import (
    get_jdbc_load_path_uri,
    get_postgresql_load_path_uri,
    get_redshift_load_path_uri,
)


def test_connection_mockup(connection):
    assert connection
    assert connection.host == "my-private-instance.com"
    assert connection.login == "airflow-user"
    assert connection.password == "XXXXXXXX"
    assert connection.port == 5439


def test_get_redshift_load_path_uri(connection):
    assert (
        get_redshift_load_path_uri(connection_id='my_conn_id')
        == "jdbc:redshift://my-private-instance.com:5439/db_client?user=airflow-user&password=XXXXXXXX&ssl=true"  # noqa: E501 W503
    )


def test_get_postgresql_load_path_uri(connection):
    assert (
        get_postgresql_load_path_uri(connection_id='my_conn_id')
        == "jdbc:postgresql://my-private-instance.com:5439/db_client?user=airflow-user&password=XXXXXXXX&ssl=true"  # noqa: E501 W503
    )


def my_decorator(conn: Connection) -> Connection:
    conn.password = 'YYY'
    return conn


def test_get_jdbc_load_path_uri_with_decorator(connection):
    assert (
        get_jdbc_load_path_uri(jdbc_type="test", connection_id='my_conn_id', decorator=my_decorator)
        == "jdbc:test://my-private-instance.com:5439/db_client?user=airflow-user&password=YYY&ssl=true"  # noqa:  W503
    )
