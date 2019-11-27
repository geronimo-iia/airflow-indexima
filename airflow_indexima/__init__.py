"""airflow-indexima definition."""
from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution('airflow_indexima').version
except DistributionNotFound:
    __version__ = '(local)'

from airflow_indexima.hook import IndeximaHook
from airflow_indexima.operator import (
    IndeximaHookBasedOperator,
    IndeximaQueryRunnerOperator,
    IndeximaLoadDataOperator,
)

__all__ = [
    'IndeximaHook',
    'IndeximaHookBasedOperator',
    'IndeximaQueryRunnerOperator',
    'IndeximaLoadDataOperator',
]
