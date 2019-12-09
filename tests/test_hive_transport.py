import pytest

from airflow_indexima.hive_transport import check_hive_connection_parameters, create_transport_socket


def test_create_transport_socket():
    socket = create_transport_socket(host='localhost', port=10000)
    assert socket.host == 'localhost'
    assert socket.port == 10000


def test_create_transport_socket_with_keep_alive():
    socket = create_transport_socket(host='localhost', port=10000, socket_keepalive=True)
    assert socket.host == 'localhost'
    assert socket.port == 10000


def test_check_hive_connection_parameters():

    with pytest.raises(ValueError):
        check_hive_connection_parameters()

    with pytest.raises(ValueError):
        check_hive_connection_parameters(auth='ahah')

    check_hive_connection_parameters(auth='NONE')

    with pytest.raises(ValueError):
        check_hive_connection_parameters(auth='CUSTOM')
    check_hive_connection_parameters(auth='CUSTOM', password="test")

    with pytest.raises(ValueError):
        check_hive_connection_parameters(auth='LDAP')
    check_hive_connection_parameters(auth='LDAP', password="test")

    with pytest.raises(ValueError):
        check_hive_connection_parameters(auth='KERBEROS')
        check_hive_connection_parameters(auth='KERBEROS', kerberos_service_name=None)

    check_hive_connection_parameters(auth='KERBEROS', kerberos_service_name="my-service")
