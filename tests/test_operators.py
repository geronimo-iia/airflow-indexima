def test_indexima_load_data_operator_exists():
    from airflow_indexima.operators.indexima import IndeximaLoadDataOperator

    assert IndeximaLoadDataOperator


def test_indexima_query_runner_operator_exists():
    from airflow_indexima.operators.indexima import IndeximaQueryRunnerOperator

    assert IndeximaQueryRunnerOperator
