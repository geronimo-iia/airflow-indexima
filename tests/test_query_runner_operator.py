import pytest
from airflow.exceptions import AirflowException


def test_query_runner_operator_sql_query_mandatory():
    from airflow_indexima.operators.indexima import IndeximaQueryRunnerOperator

    with pytest.raises(AirflowException):
        IndeximaQueryRunnerOperator(task_id="my_task", indexima_conn_id='fake_connection_id')


def test_query_runner_operator_sql_query_has_macro_support():
    from airflow_indexima.operators.indexima import IndeximaQueryRunnerOperator

    assert '_sql_query' in IndeximaQueryRunnerOperator.template_fields
