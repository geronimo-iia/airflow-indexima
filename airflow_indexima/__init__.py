"""airflow-indexima definition.

This root module expose:

 - IndeximaHook
 - IndeximaHookBasedOperator
 - IndeximaQueryRunnerOperator
 - IndeximaLoadDataOperator
"""
from pkg_resources import DistributionNotFound, get_distribution

from airflow_indexima.hook import IndeximaHook
from airflow_indexima.operator import (
    IndeximaHookBasedOperator,
    IndeximaLoadDataOperator,
    IndeximaQueryRunnerOperator,
)


try:
    __version__ = get_distribution('airflow_indexima').version
except DistributionNotFound:
    __version__ = '(local)'


__all__ = [
    'IndeximaHook',
    'IndeximaHookBasedOperator',
    'IndeximaQueryRunnerOperator',
    'IndeximaLoadDataOperator',
]
