"""Define Indexima Airflow plugin.

This will create a hook, and an operator accessible at:

- airflow.hooks.indexima.IndeximaHook
- airflow.operators.indexima.IndeximaQueryRunnerOperator
- airflow.operators.indexima.IndeximaLoadDataOperator


see https://airflow.apache.org/docs/stable/plugins.html
"""
from airflow.plugins_manager import AirflowPlugin

from airflow_indexima.hooks.indexima import IndeximaHook
from airflow_indexima.operators.indexima import IndeximaLoadDataOperator, IndeximaQueryRunnerOperator


class IndeximaAirflowPlugin(AirflowPlugin):
    name = 'indexima'
    operators = [IndeximaQueryRunnerOperator, IndeximaLoadDataOperator]
    hooks = [IndeximaHook]
