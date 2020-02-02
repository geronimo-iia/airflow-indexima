"""Define Indexima Airflow plugin.

This will create a hook, and an operator accessible at:

- airflow.hooks.indexima.IndeximaHook
- airflow.operators.indexima.IndeximaQueryRunnerOperator
- airflow.operators.indexima.IndeximaLoadDataOperator


see https://airflow.apache.org/docs/stable/plugins.html

"""
from airflow.plugins_manager import AirflowPlugin

from .hooks.indexima import IndeximaHook
from .operators.indexima import IndeximaLoadDataOperator, IndeximaQueryRunnerOperator

__all__ = ["IndeximaAirflowPlugin"]


class IndeximaAirflowPlugin(AirflowPlugin):
    """Indexima Airflow Plugin Declaration."""

    name = 'indexima'
    operators = [IndeximaQueryRunnerOperator, IndeximaLoadDataOperator]
    hooks = [IndeximaHook]
