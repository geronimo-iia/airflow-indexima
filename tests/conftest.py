"""Unit tests configuration file."""
import logging
import os

import pytest
from airflow.hooks.base_hook import BaseHook


def pytest_configure(config):
    """Disable verbose output when running tests."""
    _logger = logging.getLogger()
    _logger.setLevel(logging.DEBUG)

    terminal = config.pluginmanager.getplugin('terminal')
    terminal.TerminalReporter.showfspath = False


@pytest.fixture(scope="module")
def connection():
    os.environ[
        'AIRFLOW_CONN_MY_CONN_ID'
    ] = 'redshift://airflow-user:XXXXXXXX@my-private-instance.com:5439/db_client?ssl=true'
    yield BaseHook.get_connection('my_conn_id')
    del os.environ['AIRFLOW_CONN_MY_CONN_ID']
