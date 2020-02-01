from airflow_indexima.connection import apply_hive_extra_setting, extract_hive_extra_setting


def test_apply_hive_extra_setting_with_nothing(indexima_connection):

    assert indexima_connection.extra is None
    conn = apply_hive_extra_setting(connection=indexima_connection)
    assert conn.extra == "{}"


def test_apply_hive_extra_setting_with_attribute_1(indexima_connection):

    conn = apply_hive_extra_setting(
        connection=indexima_connection,
        auth="CUSTOM",
        kerberos_service_name="my_service",
        timeout_seconds=90,
        socket_keepalive=True,
    )
    assert conn.extra == (
        '{"auth": "CUSTOM", "kerberos_service_name": "my_service", ' '"timeout_seconds": 90, "socket_keepalive": true}'
    )


def test_apply_hive_extra_setting_with_attribute_2(indexima_connection):
    conn = apply_hive_extra_setting(
        connection=indexima_connection,
        auth="NONE",
        kerberos_service_name="my_service",
        timeout_seconds=91,
        socket_keepalive=False,
    )
    assert conn.extra == (
        '{"auth": "NONE", "kerberos_service_name": "my_service", ' '"timeout_seconds": 91, "socket_keepalive": false}'
    )


def test_extract_hive_extra_setting_1(indexima_connection):
    conn = apply_hive_extra_setting(
        connection=indexima_connection,
        auth="NONE",
        kerberos_service_name="my_service",
        timeout_seconds=91,
        socket_keepalive=False,
    )
    assert extract_hive_extra_setting(connection=conn) == ('NONE', 'my_service', 91, False)


def test_extract_hive_extra_setting_2(indexima_connection):
    conn = apply_hive_extra_setting(
        connection=indexima_connection, auth="LDAP", timeout_seconds=91, socket_keepalive=True
    )
    assert extract_hive_extra_setting(connection=conn) == ('LDAP', None, 91, True)


def test_extract_hive_extra_setting_without_data_1(indexima_connection):
    conn = apply_hive_extra_setting(connection=indexima_connection, timeout_seconds=90)
    assert extract_hive_extra_setting(connection=conn) == (None, None, 90, None)


def test_extract_hive_extra_setting_without_data_2(indexima_connection):
    conn = apply_hive_extra_setting(connection=indexima_connection)
    assert extract_hive_extra_setting(connection=conn) == (None, None, None, None)
