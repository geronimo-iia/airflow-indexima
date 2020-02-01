from airflow.models import Connection

from airflow_indexima.uri import define_load_path_factory, get_redshift_load_path_uri


def my_decorator(conn: Connection) -> Connection:
    conn.password = 'YYY'
    conn.login = "oops"
    return conn


def test_define_load_path_factory_with_decorator(connection):
    func = define_load_path_factory(conn_id="my_conn_id", decorator=my_decorator, factory=get_redshift_load_path_uri)

    assert func
    assert func() == "jdbc:redshift://my-private-instance.com:5439/db_client?user=oops&password=YYY&ssl=true"
    assert func() == "jdbc:redshift://my-private-instance.com:5439/db_client?user=oops&password=YYY&ssl=true"
