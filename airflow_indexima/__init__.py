"""airflow-indexima definition."""
from pkg_resources import DistributionNotFound, get_distribution


try:
    __version__ = get_distribution('airflow_indexima').version
except DistributionNotFound:
    __version__ = '(local)'
